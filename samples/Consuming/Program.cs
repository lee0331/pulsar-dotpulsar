/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Consuming;

using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Newtonsoft.Json;
using System;
using System.Threading;
using System.Threading.Tasks;

internal static class Program
{
    private static async Task Main()
    {
        var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, args) =>
        {
            cts.Cancel();
            args.Cancel = true;
        };

        await using var client = PulsarClient.Builder()
            // .ServiceUrl(new Uri("pulsar://120.53.200.171:10007"))
            .ServiceUrl(new Uri("http://pulsar-47xvox7w48e4.tdmq.ap-bj.public.tencenttdmq.com:8080"))
            .Authentication(AuthenticationFactory.Token("eyJrZXlJZCI6InB1bHNhci00N3h2b3g3dzQ4ZTQiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWxzYXItNDd4dm94N3c0OGU0X3ljaCJ9.7R1GgocZ4Y5ca7b5lQ6HmDBiVgF-LmredQcnS6-OCZM"))
            .ExceptionHandler(ec => Console.WriteLine($"Exception: {ec.Exception}"))

            .Build(); // Connecting to pulsar://localhost:6650

        await using var consumer = client.NewConsumer(Schema.String)
            .StateChangedHandler(Monitor)
            .SubscriptionName("lee_test_sub")
            //TODO 当前确实分区确认步骤，仅支持一个分区订阅  -partition-0
            .Topic("persistent://pulsar-47xvox7w48e4/data_uplink/core_data-partition-0")
            .SubscriptionType(SubscriptionType.KeyShared)
            .InitialPosition(SubscriptionInitialPosition.Earliest)

            .Create();

        Console.WriteLine("Press Ctrl+C to exit");

        await ConsumeMessages(consumer, cts.Token);
    }

    private static async Task ConsumeMessages(IConsumer<string> consumer, CancellationToken cancellationToken)
    {
        try
        {

            await foreach (var message in consumer.Messages(cancellationToken))
            {
                string prop = JsonConvert.SerializeObject(message.Properties);
                Console.WriteLine($"Received prop: {prop}");
                Console.WriteLine($"Received: {message.Value()}");
                await consumer.Acknowledge(message, cancellationToken);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            // consumer.Unsubscribe();
        }
    }

    private static void Monitor(ConsumerStateChanged stateChanged)
    {
        var topic = stateChanged.Consumer.Topic;
        var state = stateChanged.ConsumerState;
        Console.WriteLine($"The consumer for topic '{topic}' changed state to '{state}'");
    }
}
