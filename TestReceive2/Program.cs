using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace TestReceive2
{
    class Program
    {
        public static void Integrate(object o, BasicDeliverEventArgs e)
        {
            DoIntegrate(e.Body, o);
        }

        public static void DoIntegrate(byte[] rawBody, object o = null)
        {
            BinaryFormatter Bf = new BinaryFormatter();
            string result = System.Text.Encoding.UTF8.GetString(rawBody);
            System.Console.WriteLine("收到訊息" + result);
        }

        static void Main(string[] args)
        {
            var modvalue = 9000000 % 50001;
            var cntvalue = 9000000 / 50001;
            return;
            //Log.Info($"{Section.Get.Common.Name} service start");
            //System.Console.WriteLine("service start");
            //MqWapper.Instance().Start();
            //MqWapper.Instance().RegistByTopic(true, "nonoe", "nono", Integrate);
            //System.Console.ReadLine();

            ConnectionFactory factory = new ConnectionFactory();
            // "guest"/"guest" by default, limited to localhost connections
            factory.UserName = "nono";
            factory.Password = "nono123";
            factory.HostName = "192.168.209.11";
            factory.Port = 5672;
            //IConnection conn = factory.CreateConnection(new string[1] { "192.168.4.51" });

            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel model = connection.CreateModel())
                {
                    var subscription = new Subscription(model, "fq2", false);
                    while (true)
                    {
                        BasicDeliverEventArgs basicDeliveryEventArgs =
                            subscription.Next();
                        string messageContent =
                            Encoding.UTF8.GetString(basicDeliveryEventArgs.Body);
                        Console.WriteLine(messageContent);
                        subscription.Ack(basicDeliveryEventArgs);
                        System.Threading.Thread.Sleep(300);
                    }
                }
            }
            System.Console.ReadLine();
        }
    }
}
