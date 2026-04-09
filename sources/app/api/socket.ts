import { onShutdown } from "@/utils/shutdown";
import { Fastify } from "./types";
import { buildMachineActivityEphemeral, buildSessionActivityEphemeral, ClientConnection, eventRouter } from "@/app/events/eventRouter";
import { Server, Socket } from "socket.io";
import { log } from "@/utils/log";
import { auth } from "@/app/auth/auth";
import { db } from "@/storage/db";
import { decrementWebSocketConnection, incrementWebSocketConnection, websocketEventsCounter } from "../monitoring/metrics2";
import { usageHandler } from "./socket/usageHandler";
import { rpcHandler } from "./socket/rpcHandler";
import { pingHandler } from "./socket/pingHandler";
import { sessionUpdateHandler } from "./socket/sessionUpdateHandler";
import { machineUpdateHandler } from "./socket/machineUpdateHandler";
import { artifactUpdateHandler } from "./socket/artifactUpdateHandler";
import { accessKeyHandler } from "./socket/accessKeyHandler";
import { getSocketCorsConfig } from "./utils/corsConfig";
import { activityCache } from "@/app/presence/sessionCache";
import { observeSessionActivity } from "@/app/presence/observeSessionActivity";

export function startSocket(app: Fastify) {
    const io = new Server(app.server, {
        cors: getSocketCorsConfig(),
        transports: ['websocket', 'polling'],
        pingTimeout: 45000,
        pingInterval: 15000,
        path: '/v1/updates',
        allowUpgrades: true,
        upgradeTimeout: 10000,
        connectTimeout: 20000,
        serveClient: false // Don't serve the client files
    });

    let rpcListeners = new Map<string, Map<string, Socket>>();
    io.on("connection", async (socket) => {
        log({ module: 'websocket' }, `New connection attempt from socket: ${socket.id}`);
        const token = socket.handshake.auth.token as string;
        const clientType = socket.handshake.auth.clientType as 'session-scoped' | 'user-scoped' | 'machine-scoped' | undefined;
        const sessionId = socket.handshake.auth.sessionId as string | undefined;
        const machineId = socket.handshake.auth.machineId as string | undefined;

        if (!token) {
            log({ module: 'websocket' }, `No token provided`);
            socket.emit('error', { message: 'Missing authentication token' });
            socket.disconnect();
            return;
        }

        // Validate session-scoped clients have sessionId
        if (clientType === 'session-scoped' && !sessionId) {
            log({ module: 'websocket' }, `Session-scoped client missing sessionId`);
            socket.emit('error', { message: 'Session ID required for session-scoped clients' });
            socket.disconnect();
            return;
        }

        // Validate machine-scoped clients have machineId
        if (clientType === 'machine-scoped' && !machineId) {
            log({ module: 'websocket' }, `Machine-scoped client missing machineId`);
            socket.emit('error', { message: 'Machine ID required for machine-scoped clients' });
            socket.disconnect();
            return;
        }

        const verified = await auth.verifyToken(token);
        if (!verified) {
            log({ module: 'websocket' }, `Invalid token provided`);
            socket.emit('error', { message: 'Invalid authentication token' });
            socket.disconnect();
            return;
        }

        const userId = verified.userId;
        const account = await db.account.findUnique({
            where: { id: userId },
            select: { id: true },
        });
        if (!account) {
            log({ module: 'websocket', level: 'error', userId }, 'Token points to a missing account');
            socket.emit('error', { message: 'Account not found for token' });
            socket.disconnect();
            return;
        }
        log({ module: 'websocket' }, `Token verified: ${userId}, clientType: ${clientType || 'user-scoped'}, sessionId: ${sessionId || 'none'}, machineId: ${machineId || 'none'}, socketId: ${socket.id}`);

        // Store connection based on type
        const metadata = { clientType: clientType || 'user-scoped', sessionId, machineId };
        let connection: ClientConnection;
        if (metadata.clientType === 'session-scoped' && sessionId) {
            connection = {
                connectionType: 'session-scoped',
                socket,
                userId,
                sessionId
            };
        } else if (metadata.clientType === 'machine-scoped' && machineId) {
            connection = {
                connectionType: 'machine-scoped',
                socket,
                userId,
                machineId
            };
        } else {
            connection = {
                connectionType: 'user-scoped',
                socket,
                userId
            };
        }
        eventRouter.addConnection(userId, connection);
        incrementWebSocketConnection(connection.connectionType);

        if (connection.connectionType === 'session-scoped') {
            await observeSessionActivity(userId, connection.sessionId);
        }

        // Broadcast daemon online status
        if (connection.connectionType === 'machine-scoped') {
            try {
                await db.machine.update({
                    where: {
                        accountId_id: {
                            accountId: userId,
                            id: machineId!
                        }
                    },
                    data: {
                        active: true,
                        lastActiveAt: new Date()
                    }
                });
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error marking machine ${machineId} as online: ${error}`);
            }

            // Broadcast daemon online
            const machineActivity = buildMachineActivityEphemeral(machineId!, true, Date.now());
            eventRouter.emitEphemeral({
                userId,
                payload: machineActivity,
                recipientFilter: { type: 'user-scoped-only' }
            });
        }

        socket.on('disconnect', async (reason?: string) => {
            websocketEventsCounter.inc({ event_type: 'disconnect' });

            // Cleanup connections
            eventRouter.removeConnection(userId, connection);
            decrementWebSocketConnection(connection.connectionType);

            log({ module: 'websocket' }, `User disconnected: ${userId}`);

            if (reason === 'server shutting down') {
                return;
            }

            // Broadcast daemon offline status
            if (connection.connectionType === 'machine-scoped') {
                const machineActivity = buildMachineActivityEphemeral(connection.machineId, false, Date.now());
                eventRouter.emitEphemeral({
                    userId,
                    payload: machineActivity,
                    recipientFilter: { type: 'user-scoped-only' }
                });

                // Update database to mark machine as offline
                try {
                    await db.machine.update({
                        where: {
                            accountId_id: {
                                accountId: userId,
                                id: connection.machineId
                            }
                        },
                        data: {
                            active: false,
                            lastActiveAt: new Date()
                        }
                    });
                    log({ module: 'websocket' }, `Machine ${connection.machineId} marked as offline`);
                } catch (error) {
                    log({ module: 'websocket', level: 'error' }, `Error marking machine ${connection.machineId} as offline: ${error}`);
                }
            }

            if (connection.connectionType === 'session-scoped') {
                const disconnectedAt = Date.now();
                try {
                    const updated = await db.session.updateManyAndReturn({
                        where: {
                            id: connection.sessionId,
                            accountId: userId,
                            active: true,
                        },
                        data: {
                            active: false,
                            lastActiveAt: new Date(disconnectedAt),
                        },
                    });

                    if (updated.length > 0) {
                        activityCache.invalidateSession(connection.sessionId);
                        eventRouter.emitEphemeral({
                            userId,
                            payload: buildSessionActivityEphemeral(connection.sessionId, false, disconnectedAt, false),
                            recipientFilter: { type: 'user-scoped-only' },
                        });
                        log({ module: 'websocket' }, `Session ${connection.sessionId} marked as offline on disconnect`);
                    }
                } catch (error) {
                    log({ module: 'websocket', level: 'error' }, `Error marking session ${connection.sessionId} as offline: ${error}`);
                }
            }
        });

        // Handlers
        let userRpcListeners = rpcListeners.get(userId);
        if (!userRpcListeners) {
            userRpcListeners = new Map<string, Socket>();
            rpcListeners.set(userId, userRpcListeners);
        }
        rpcHandler(userId, socket, userRpcListeners);

        // After rpcHandler registers its disconnect cleanup, register our own to
        // prune the outer rpcListeners map once the user has no RPC listeners left.
        socket.on('disconnect', () => {
            const listeners = rpcListeners.get(userId);
            if (listeners && listeners.size === 0) {
                rpcListeners.delete(userId);
            }
        });
        usageHandler(userId, socket);
        sessionUpdateHandler(userId, socket, connection);
        pingHandler(socket);
        machineUpdateHandler(userId, socket);
        artifactUpdateHandler(userId, socket);
        accessKeyHandler(userId, socket);

        // Ready
        log({ module: 'websocket' }, `User connected: ${userId}`);
    });

    onShutdown('api', async () => {
        await io.close();
    });
}
