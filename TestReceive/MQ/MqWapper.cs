using jIAnSoft.Framework.Nami.Fibers;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestReceive.MQ
{
    public class MqWapper
    {
        public RabbitMqHelper Master { get; set; }
        //public RabbitMqHelper Slave { get; set; }
        //public RabbitMqHelper Third { get; set; }

        private static MqWapper _instance;
        private static IFiber _fiber;

        public static MqWapper Instance()
        {
            return _instance ?? (_instance = new MqWapper());
        }

        private MqWapper()
        {
            _fiber = new PoolFiber();
            _fiber.Start();
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["RabbitMqMaster"]))
            {
                var rabbitMqConfig = ConfigurationManager.AppSettings["RabbitMqMaster"].Split('|');
                Master = new RabbitMqHelper(
                    rabbitMqConfig[0],
                    rabbitMqConfig[2],
                    rabbitMqConfig[3],
                    int.Parse(rabbitMqConfig[1])
                );
            }

            //if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["RabbitMqSlave"]))
            //{
            //    var rabbitMqConfig = ConfigurationManager.AppSettings["RabbitMqSlave"].Split('|');
            //    Slave = new RabbitMqHelper(
            //        rabbitMqConfig[0],
            //        rabbitMqConfig[2],
            //        rabbitMqConfig[3],
            //        int.Parse(rabbitMqConfig[1])
            //    );
            //}

            //if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["RabbitMqThird"]))
            //{
            //    var rabbitMqConfig = ConfigurationManager.AppSettings["RabbitMqThird"].Split('|');
            //    Third = new RabbitMqHelper(
            //        rabbitMqConfig[0],
            //        rabbitMqConfig[2],
            //        rabbitMqConfig[3],
            //        int.Parse(rabbitMqConfig[1])
            //    );
            //}
        }

        public void Start()
        {
            Master?.Connect();
            //Slave?.Connect();
            //Third?.Connect();
        }

        public void Fanout(string key, string routingKey, string msg)
        {
            Master?.PublishMessageByFanout(key, routingKey, msg);
            //Slave?.PublishMessageByFanout(key, routingKey, msg);
            //Third?.PublishMessageByFanout(key, routingKey, msg);
        }

        public void Topic(string key, string routingKey, string msg, bool durable = false)
        {
            Topic(key, routingKey, Encoding.UTF8.GetBytes(msg), durable);
        }

        public void Topic(string key, string routingKey, byte[] msg, bool durable = false)
        {
            _fiber.Enqueue(() => { Master?.PublishMessageByTopic(key, routingKey, msg, durable); });
            //_fiber.Enqueue(() => { Slave?.PublishMessageByTopic(key, routingKey, msg, durable); });
            //_fiber.Enqueue(() => { Third?.PublishMessageByTopic(key, routingKey, msg, durable); });
        }

        public void RegistByTopic(bool durable, string exchange, string routingKey,
            EventHandler<BasicDeliverEventArgs> eventHandler)
        {
            _fiber.Enqueue(() => { Master?.RegistConsumerByTopic(durable, exchange, routingKey, eventHandler); });
            //_fiber.Enqueue(() => { Slave?.RegistConsumerByTopic(durable, exchange, routingKey, eventHandler); });
            //_fiber.Enqueue(() => { Third?.RegistConsumerByTopic(durable, exchange, routingKey, eventHandler); });
        }
    }
}
