using NLog;
using RabbitMQ.Client;
using UtilityLibrary.Models;
namespace TestSend
{
    class Program
    {
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();
        static void Main(string[] args)
        {
            /*
            Log.Info("service start");
            MqWapper.Instance().Start();
            System.Threading.Thread.Sleep(2000);
            for (int i = 0; i <= 999999; i++)
            {
                MqWapper.Instance().Topic("thankQQ", "thankyou", i.ToString());
                System.Console.WriteLine(i.ToString());
            }
            */

            
            ConnectionFactory factory = new ConnectionFactory();
            // "guest"/"guest" by default, limited to localhost connections
            factory.UserName = "nono";
            factory.Password = "nono123";
            factory.HostName = "192.168.209.11";
            factory.Port = 5672;
            //IConnection conn = factory.CreateConnection();
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    for (int i = 0; i <= 999999; i++)
                    {
                        byte[] messageBodyBytes = System.Text.Encoding.UTF8.GetBytes(i.ToString());
                        channel.ExchangeDeclare("thankQQ", ExchangeType.Topic, true);
                        channel.BasicPublish("thankQQ", "thankyou", new RabbitMQ.Client.Framing.BasicProperties { Persistent = true }, messageBodyBytes);
                        System.Console.WriteLine(i.ToString());
                        //System.Threading.Thread.Sleep(200);
                    }

                    channel.Close();
                    connection.Close();
                }
            }
            System.Console.ReadLine();
            
        }
    }
}
