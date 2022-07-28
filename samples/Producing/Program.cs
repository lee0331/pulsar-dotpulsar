﻿/*
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


        await using var producer = client.NewProducer(Schema.String)
            .StateChangedHandler(Monitor)
            .Topic("persistent://pulsar-47xvox7w48e4/data_uplink/core_data")
            .Create();

        Console.WriteLine("Press Ctrl+C to exit");

        await ProduceMessages(producer, cts.Token);
    }

    private static async Task ProduceMessages(IProducer<string> producer, CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromSeconds(5);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var data = "Lee test msg " + DateTime.UtcNow.ToLongTimeString();
                Console.WriteLine($"start Sent: {data}");
                _ = await producer.NewMessage().Property("tablename","mall_leaguer").Property("shopid","1000010").Send(data, cancellationToken);
                Console.WriteLine($"Sent: {data}");
                await Task.Delay(delay, cancellationToken);
            }
        }
        catch (OperationCanceledException) // If not using the cancellationToken, then just dispose the producer and catch ObjectDisposedException instead
        { }
    }

    private static void Monitor(ProducerStateChanged stateChanged)
    {
        var topic = stateChanged.Producer.Topic;
        var state = stateChanged.ProducerState;
        Console.WriteLine($"The producer for topic '{topic}' changed state to '{state}'");
    }
}
