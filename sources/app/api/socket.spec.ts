import { beforeEach, describe, expect, it, vi } from 'vitest';

const {
    addConnection,
    removeConnection,
    emitEphemeral,
    incrementWebSocketConnection,
    decrementWebSocketConnection,
    websocketEventsCounterInc,
    verifyToken,
    accountFindUnique,
    sessionUpdateManyAndReturn,
    invalidateSession,
    observeSessionActivity,
} = vi.hoisted(() => ({
    addConnection: vi.fn(),
    removeConnection: vi.fn(),
    emitEphemeral: vi.fn(),
    incrementWebSocketConnection: vi.fn(),
    decrementWebSocketConnection: vi.fn(),
    websocketEventsCounterInc: vi.fn(),
    verifyToken: vi.fn(),
    accountFindUnique: vi.fn(),
    sessionUpdateManyAndReturn: vi.fn(),
    invalidateSession: vi.fn(),
    observeSessionActivity: vi.fn(),
}));

let lastServerOptions: any;
let connectionHandler: ((socket: FakeSocket) => Promise<void>) | undefined;

class FakeSocket {
    id: string;
    handshake: { auth: Record<string, any> };
    emitted: Array<{ event: string; payload: any }> = [];
    disconnect = vi.fn();
    private handlers = new Map<string, Array<(...args: any[]) => any>>();

    constructor(auth: Record<string, any>, id = 'socket-1') {
        this.id = id;
        this.handshake = { auth };
    }

    on(event: string, handler: (...args: any[]) => any) {
        const existing = this.handlers.get(event) || [];
        existing.push(handler);
        this.handlers.set(event, existing);
        return this;
    }

    emit(event: string, payload: any) {
        this.emitted.push({ event, payload });
        return true;
    }

    async trigger(event: string, ...args: any[]) {
        const handlers = this.handlers.get(event) || [];
        for (const handler of handlers) {
            await handler(...args);
        }
    }
}

vi.mock('socket.io', () => {
    class MockServer {
        constructor(_server: unknown, options: unknown) {
            lastServerOptions = options;
        }

        on(event: string, handler: (socket: FakeSocket) => Promise<void>) {
            if (event === 'connection') {
                connectionHandler = handler;
            }
            return this;
        }

        close = vi.fn(async () => undefined);
    }

    return {
        Server: MockServer,
    };
});

vi.mock('@/utils/shutdown', () => ({
    onShutdown: vi.fn(),
}));

vi.mock('@/app/events/eventRouter', () => ({
    eventRouter: {
        addConnection,
        removeConnection,
        emitEphemeral,
    },
    buildSessionActivityEphemeral: (sessionId: string, active: boolean, activeAt: number, thinking?: boolean) => ({
        type: 'activity',
        id: sessionId,
        active,
        activeAt,
        thinking: thinking || false,
    }),
    buildMachineActivityEphemeral: (machineId: string, active: boolean, activeAt: number) => ({
        type: 'machine-activity',
        id: machineId,
        active,
        activeAt,
    }),
}));

vi.mock('@/app/auth/auth', () => ({
    auth: {
        verifyToken,
    },
}));

vi.mock('@/storage/db', () => ({
    db: {
        account: {
            findUnique: accountFindUnique,
        },
        session: {
            updateManyAndReturn: sessionUpdateManyAndReturn,
        },
        machine: {
            update: vi.fn(),
        },
    },
}));

vi.mock('@/app/presence/sessionCache', () => ({
    activityCache: {
        invalidateSession,
    },
}));

vi.mock('@/app/presence/observeSessionActivity', () => ({
    observeSessionActivity,
}));

vi.mock('../monitoring/metrics2', () => ({
    incrementWebSocketConnection,
    decrementWebSocketConnection,
    websocketEventsCounter: {
        inc: websocketEventsCounterInc,
    },
}));

vi.mock('./socket/usageHandler', () => ({ usageHandler: vi.fn() }));
vi.mock('./socket/rpcHandler', () => ({ rpcHandler: vi.fn() }));
vi.mock('./socket/pingHandler', () => ({ pingHandler: vi.fn() }));
vi.mock('./socket/sessionUpdateHandler', () => ({ sessionUpdateHandler: vi.fn() }));
vi.mock('./socket/machineUpdateHandler', () => ({ machineUpdateHandler: vi.fn() }));
vi.mock('./socket/artifactUpdateHandler', () => ({ artifactUpdateHandler: vi.fn() }));
vi.mock('./socket/accessKeyHandler', () => ({ accessKeyHandler: vi.fn() }));
vi.mock('./utils/corsConfig', () => ({
    getSocketCorsConfig: vi.fn(() => ({ origin: '*' })),
}));

import { startSocket } from './socket';

describe('startSocket', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        lastServerOptions = undefined;
        connectionHandler = undefined;

        verifyToken.mockResolvedValue({ userId: 'user-1' });
        accountFindUnique.mockResolvedValue({ id: 'user-1' });
        sessionUpdateManyAndReturn.mockResolvedValue([{ id: 'session-1' }]);
        observeSessionActivity.mockResolvedValue(true);
    });

    it('configures socket.io stale cleanup with 45s pingTimeout and 15s pingInterval', () => {
        startSocket({ server: {} } as any);

        expect(lastServerOptions).toMatchObject({
            pingTimeout: 45000,
            pingInterval: 15000,
            transports: ['websocket', 'polling'],
            path: '/v1/updates',
        });
    });

    it('marks a session-scoped connection active again on reconnect', async () => {
        startSocket({ server: {} } as any);
        const socket = new FakeSocket({
            token: 'valid-token',
            clientType: 'session-scoped',
            sessionId: 'session-1',
        });

        await connectionHandler!(socket);

        expect(addConnection).toHaveBeenCalledWith('user-1', expect.objectContaining({
            connectionType: 'session-scoped',
            sessionId: 'session-1',
        }));
        expect(observeSessionActivity).toHaveBeenCalledWith('user-1', 'session-1');
    });

    it('marks session active=false and broadcasts offline activity on disconnect', async () => {
        startSocket({ server: {} } as any);
        const socket = new FakeSocket({
            token: 'valid-token',
            clientType: 'session-scoped',
            sessionId: 'session-1',
        });

        await connectionHandler!(socket);
        vi.clearAllMocks();
        sessionUpdateManyAndReturn.mockResolvedValue([{ id: 'session-1' }]);

        await socket.trigger('disconnect');

        expect(removeConnection).toHaveBeenCalledWith('user-1', expect.objectContaining({
            connectionType: 'session-scoped',
            sessionId: 'session-1',
        }));
        expect(sessionUpdateManyAndReturn).toHaveBeenCalledWith({
            where: {
                id: 'session-1',
                accountId: 'user-1',
                active: true,
            },
            data: {
                active: false,
                lastActiveAt: expect.any(Date),
            },
        });
        expect(invalidateSession).toHaveBeenCalledWith('session-1');
        expect(emitEphemeral).toHaveBeenCalledWith({
            userId: 'user-1',
            payload: expect.objectContaining({
                type: 'activity',
                id: 'session-1',
                active: false,
            }),
            recipientFilter: { type: 'user-scoped-only' },
        });
    });

    it('does not broadcast offline activity when disconnect does not change any active session row', async () => {
        startSocket({ server: {} } as any);
        const socket = new FakeSocket({
            token: 'valid-token',
            clientType: 'session-scoped',
            sessionId: 'session-1',
        });

        await connectionHandler!(socket);
        vi.clearAllMocks();
        sessionUpdateManyAndReturn.mockResolvedValue([]);

        await socket.trigger('disconnect');

        expect(emitEphemeral).not.toHaveBeenCalled();
        expect(invalidateSession).not.toHaveBeenCalled();
    });

    it('skips offline updates during graceful server shutdown disconnects', async () => {
        startSocket({ server: {} } as any);
        const socket = new FakeSocket({
            token: 'valid-token',
            clientType: 'session-scoped',
            sessionId: 'session-1',
        });

        await connectionHandler!(socket);
        vi.clearAllMocks();

        await socket.trigger('disconnect', 'server shutting down');

        expect(removeConnection).toHaveBeenCalledWith('user-1', expect.objectContaining({
            connectionType: 'session-scoped',
            sessionId: 'session-1',
        }));
        expect(sessionUpdateManyAndReturn).not.toHaveBeenCalled();
        expect(invalidateSession).not.toHaveBeenCalled();
        expect(emitEphemeral).not.toHaveBeenCalled();
    });

    it('rejects session-scoped reconnect when the auth token is invalid', async () => {
        verifyToken.mockResolvedValue(null);
        startSocket({ server: {} } as any);
        const socket = new FakeSocket({
            token: 'bad-token',
            clientType: 'session-scoped',
            sessionId: 'session-1',
        });

        await connectionHandler!(socket);

        expect(socket.emitted).toContainEqual({
            event: 'error',
            payload: { message: 'Invalid authentication token' },
        });
        expect(socket.disconnect).toHaveBeenCalled();
        expect(addConnection).not.toHaveBeenCalled();
    });

    it('rejects session-scoped reconnect without sessionId', async () => {
        startSocket({ server: {} } as any);
        const socket = new FakeSocket({
            token: 'valid-token',
            clientType: 'session-scoped',
        });

        await connectionHandler!(socket);

        expect(socket.emitted).toContainEqual({
            event: 'error',
            payload: { message: 'Session ID required for session-scoped clients' },
        });
        expect(socket.disconnect).toHaveBeenCalled();
        expect(verifyToken).not.toHaveBeenCalled();
    });
});
