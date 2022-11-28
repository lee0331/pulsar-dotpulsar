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

namespace Producing;

using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using DotPulsar.Internal;
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

        DotPulsar.Internal.Constants.fromip = "169.254.0.168";
        DotPulsar.Internal.Constants.proxyip = "172.20.226.143";


        // await using var client = PulsarClient.Builder()
        //
        //
        //     //公网地址
        //     // .ServiceUrl(new Uri("http://pulsar-***.tdmq.ap-bj.public.tencenttdmq.com:8080"))
        //     //内网地址
        //     .ServiceUrl(new Uri("http://pulsar-.tdmq.ap-bj.qcloud.tencenttdmq.com:5006"))
        //     .Authentication(AuthenticationFactory.Token("..-LJI"))
        //     .ExceptionHandler(ec => Console.WriteLine($"Exception: {ec.Exception}"))
        //     .Build(); // Connecting to pulsar://localhost:6650

        await using var client2 = PulsarClient.Builder()


            //公网地址
            // .ServiceUrl(new Uri("http://pulsar-***.tdmq.ap-bj.public.tencenttdmq.com:8080"))
            //内网地址
            .ServiceUrl(new Uri("http://pulsar.tdmq.ap-bj.qcloud.tencenttdmq.com:5006"))
            .Authentication(AuthenticationFactory.Token(".."))

            .ExceptionHandler(ec => Console.WriteLine($"Exception: {ec.Exception}"))
            .Build(); // Connecting to pulsar://localhost:6650


        // await using var consumer = client.NewConsumer(Schema.String)
        //      .StateChangedHandler(Monitor)
        //     .SubscriptionName("ykt_center_non_core_cons_lee" )
        //     //TODO 当前确实分区确认步骤，仅支持一个分区订阅  -partition-0
        //     .Topic("persistent://pulsar-/data_uplink/non_core_data-partition-0")
        //     .SubscriptionType(SubscriptionType.KeyShared)
        //
        //     .InitialPosition(SubscriptionInitialPosition.Earliest)
        //     .Create();
        //
        //
        // await using var consumer2 = client2.NewConsumer(Schema.String)
        //     .StateChangedHandler(Monitor)
        //     .SubscriptionName("ykt_center_non_core_cons_lee" )
        //     //TODO 当前确实分区确认步骤，仅支持一个分区订阅  -partition-0
        //     .Topic("persistent://pulsar-/data_uplink/non_core_data-partition-0")
        //     .SubscriptionType(SubscriptionType.KeyShared)
        //     .InitialPosition(SubscriptionInitialPosition.Earliest)
        //     .Create();



        await using var producer = client2.NewProducer(Schema.String)
            .StateChangedHandler(Monitor)
            .Topic("persistent://pulsar-/data_uplink/test_data")
            .Create();
        // producer.

       //  Console.WriteLine("Press Ctrl+C to exit");
       //    ConsumeMessages(consumer, cts.Token);
       // await ConsumeMessages2(consumer2, cts.Token);
         await ProduceMessages(producer, cts.Token);



    }

    private static async Task ProduceMessages(IProducer<string> producer, CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromMilliseconds(50);
        int i = 0;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {

                var data = "Lee test msg " + DateTime.Now.ToLongTimeString()+"_"+i++;
                Console.WriteLine($"start Sent: {data}");
                await producer.NewMessage().
                    Property("tablename","mall_leaguer").
                    Property("shopid","1000010").
                    Send(data, cancellationToken);
                Console.WriteLine($"Sent: {data}");
                Thread.Sleep(1000);
                 await Task.Delay(delay, cancellationToken);
            }
        }
        catch (OperationCanceledException) // If not using the cancellationToken, then just dispose the producer and catch ObjectDisposedException instead
        { }
    }
    private static async Task ConsumeMessages(IConsumer<string> consumer, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in consumer.Messages(cancellationToken))
            {
                var messageID = "123";
                string prop = JsonConvert.SerializeObject(message.Properties);
                Console.WriteLine($"1111Received prop: {prop}");
                Console.WriteLine($"1111Received: {message.Value()}");
                Console.WriteLine($"1111Received messageId: { message.MessageId.ToString()}");
                Task.Delay(5 * 1000, cancellationToken);
                await consumer.Acknowledge(message);

            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            // consumer.Unsubscribe();
        }
    }

    private static async Task ConsumeMessages2(IConsumer<string> consumer, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in consumer.Messages(cancellationToken))
            {
                var messageID = "123";
                string prop = JsonConvert.SerializeObject(message.Properties);
                Console.WriteLine($"2222Received prop: {prop}");
                Console.WriteLine($"2222Received: {message.Value()}");
                Console.WriteLine($"2222Received messageId: { message.MessageId.ToString()}");
                Task.Delay(5 * 1000, cancellationToken);

                await consumer.Acknowledge(message, cancellationToken);

            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            // consumer.Unsubscribe();
        }
    }
    private static void Monitor(ProducerStateChanged stateChanged)
    {
        var topic = stateChanged.Producer.Topic;
        var state = stateChanged.ProducerState;
        Console.WriteLine($"The producer for topic '{topic}' changed state to '{state}'");
    }
    private static void Monitor(ConsumerStateChanged stateChanged)
    {
        var topic = stateChanged.Consumer.Topic;
        var state = stateChanged.ConsumerState;
        Console.WriteLine($"The consumer for topic '{topic}' changed state to '{state}'");
    }
}
