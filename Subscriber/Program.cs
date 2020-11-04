using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;

namespace Subscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            ConsumerConfig config = new ConsumerConfig()
            {
                BootstrapServers = "192.168.28.172:9093,192.168.28.172:9094,192.168.28.172:9095",
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false
            };


            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<string, object>(config).SetValueDeserializer(new KafkaConverter()).Build())
            {
                //c.Assign(new TopicPartition("test", new Partition(1)));//从指定的Partition订阅消息使用Assign方法
                consumer.Subscribe("mykafka");//订阅消息使用Subscribe方法
                while (!cts.IsCancellationRequested)
                {
                    var result = consumer.Consume(cts.Token);
                    if (result.Offset % 5 == 0)
                    {
                        consumer.Commit(result);//手动提交，如果上面的EnableAutoCommit=true表示自动提交，则无需调用Commit方法
                        Console.WriteLine($"recieve message:{result.Message.Value},Committed offset: {result.Offset}");
                    }
                    else
                        Console.WriteLine($"recieve message:{result.Message.Value}，Committed offset: { result.Offset }");

                }
            }
        }
    }

    public class KafkaConverter : IDeserializer<object>
    {/// <summary>
     /// 反序列化字节数据成实体数据
     /// </summary>
     /// <param name="data"></param>
     /// <param name="context"></param>
     /// <returns></returns>
        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return null;

            var json = Encoding.UTF8.GetString(data.ToArray());
            try
            {
                return JsonConvert.DeserializeObject(json);
            }
            catch
            {
                return json;
            }
        }
    }
}
