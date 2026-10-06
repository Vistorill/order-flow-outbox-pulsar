import { InMemoryBroker } from './in-memory.broker';

describe('InMemoryBroker', () => {
  it('entrega em ordem e reentrega após nack', async () => {
    const broker = new InMemoryBroker(10);
    const seen: string[] = [];
    let failOnce = true;
    await broker.subscribe({
      topic: 't',
      subscription: 's',
      handler: (m) => {
        if (m.eventId === 'b' && failOnce) {
          failOnce = false;
          return Promise.reject(new Error('falha'));
        }
        seen.push(`${m.eventId}:${m.redeliveryCount}`);
        return Promise.resolve();
      },
    });
    await broker.publish('t', {}, { eventId: 'a', key: 'k' });
    await broker.publish('t', {}, { eventId: 'b', key: 'k' });
    await broker.publish('t', {}, { eventId: 'c', key: 'k' });
    await broker.idle();

    expect(seen).toEqual(['a:0', 'c:0', 'b:1']);
    const [stats] = await broker.stats();
    expect(stats.subscriptions[0]).toMatchObject({
      acked: 3,
      redeliveries: 1,
      backlog: 0,
    });
    broker.onApplicationShutdown();
  });

  it('retém mensagens publicadas antes da assinatura', async () => {
    const broker = new InMemoryBroker();
    await broker.publish('t', { x: 1 }, { eventId: 'a', key: 'k' });
    const got: unknown[] = [];
    await broker.subscribe({
      topic: 't',
      subscription: 's',
      handler: (m) => {
        got.push(m.payload);
        return Promise.resolve();
      },
    });
    await broker.idle();
    expect(got).toEqual([{ x: 1 }]);
    broker.onApplicationShutdown();
  });
});
