using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Publisher
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                //AutoOffsetReset = AutoOffsetReset.Earliest,//显式强类型赋值配置
                BootstrapServers = "192.168.28.172:9093,192.168.28.172:9094,192.168.28.172:9095",
                Acks = Acks.Leader
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
                Console.ReadKey();
            };

            //var builder = new ProducerBuilder<string, object>(conf);
            //builder.SetValueSerializer(new KafkaConverter());//设置序列化方式
            //var producer = builder.Build();
            //producer.Produce("test", new Message<string, object>() { Key = "Test", Value = "hello world" });

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<string, object>(config).SetValueSerializer(new KafkaConverter()).Build())
            {
                try
                {
                    Console.WriteLine("请输入keys");
                    while (!cts.IsCancellationRequested)
                    {
                        var keys = Console.ReadLine();
                        var dr = p.ProduceAsync("mykafka", new Message<string, object> { Key = keys, Value = keys }, cancellationToken: cts.Token);
                        dr.ContinueWith(task =>
                        {
                            Console.WriteLine($"Producer:{p.Name} ,Topic:{keys} ，Partition:{task.Result.Partition} ，Offset:{task.Result.Offset} ");
                        });
                        p.Flush(TimeSpan.FromSeconds(5));
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }

    public class KafkaConverter : ISerializer<object>
    {
        /// <summary>
        /// 序列化数据成字节
        /// </summary>
        /// <param name="data"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public byte[] Serialize(object data, SerializationContext context)
        {
            var json = JsonConvert.SerializeObject(data);
            return Encoding.UTF8.GetBytes(json);
        }
    }
}
