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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

internal static class Program
{
    private static async Task Main()
    {
        var str = "puslsar://169.254.0.168:10001";

        str = Regex.Replace(str, "\\d+\\.{3}\\d+", "172.20.226.143");

        Console.WriteLine(str);

        var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, args) =>
        {
            cts.Cancel();
            args.Cancel = true;
        };
        DotPulsar.Internal.Constants.fromip = "169.254.0.168";
        DotPulsar.Internal.Constants.proxyip = "172.20.226.153";
        await using var client = PulsarClient.Builder()

            //内网样式 ap-bj.qcloud.tencenttdmq.com:
            .ServiceUrl(new Uri("http://pulsar-.tdmq.ap-bj.qcloud.tencenttdmq.com:5006"))
            .Authentication(AuthenticationFactory.Token("exxxx"))
            .ExceptionHandler(ec => Console.WriteLine($"Exception: {ec.Exception}"))
            .Build(); // Connecting to pulsar://localhost:6650

        await using var consumer = client.NewConsumer(Schema.String)
            .StateChangedHandler(Monitor)
            .SubscriptionName("ykt_center_core_cons")
            //TODO 当前确实分区确认步骤，仅支持一个分区订阅  -partition-0
            .Topic("persistent://pulsar-xxxx/data_uplink/core_data-partition-0")
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
                break;
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
