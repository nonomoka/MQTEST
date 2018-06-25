using jIAnSoft.Framework.Nami.TaskScheduler;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestReceive2.MQ
{
    public class RabbitMqHelper
    {
        private readonly object _lockObj = new object();
        private ConnectionFactory _factory;

        private IConnection _mqConnection;
        private IModel _channel;

        private readonly List<MqEnevt> _topic;

        private string HostName { get; }
        private string UserName { get; }
        private string Password { get; }
        private int Port { get; }
        private bool SelfClose { get; set; }

        private RabbitMqHelper()
        {
            _topic = new List<MqEnevt>();
        }

        public RabbitMqHelper(string hostName, string userName, string password, int port) : this()
        {
            HostName = hostName;
            Port = port;
            UserName = userName;
            Password = password;
        }

        public void Start()
        {
            Connect();
        }

        public void Connect()
        {
            //連線到 RabbitMQ
            if (_mqConnection != null)
            {
                if (_mqConnection.IsOpen)
                {
                    return;
                }

                _mqConnection.ConnectionShutdown -= MqConnectionShutdown;
                _mqConnection.Close();
                _mqConnection.Dispose();
                _mqConnection = null;
            }

            var rebind = _topic.ToArray();
            _topic.Clear();
            try
            {
                lock (_lockObj)
                {
                    _factory = new ConnectionFactory
                    {
                        HostName = HostName,
                        Port = Port,
                        UserName = UserName,
                        Password = Password
                    };

                    _mqConnection = _factory.CreateConnection();
                    _mqConnection.ConnectionShutdown += MqConnectionShutdown;
                    _channel = _mqConnection.CreateModel();

                    foreach (var mqEnevt in rebind)
                    {
                        RegistConsumerByTopic(mqEnevt.Durable, mqEnevt.Exchange, mqEnevt.RoutingKey,
                            mqEnevt.EventHandler);
                    }
                }

            }
            catch (Exception e)
            {
                _topic.Clear();
                _topic.AddRange(rebind);
                Nami.Delay(5).Seconds().Do(Connect);
            }
        }

        public IModel CreateChannel()
        {
            IModel channel;
            lock (_lockObj)
            {
                channel = _mqConnection.CreateModel();
            }

            return channel;
        }

        private void MqConnectionShutdown(object sender, ShutdownEventArgs s)
        {
            if (SelfClose)
            {
                //自已關閉的操作不用重連
                return;
            }

            Connect();
        }


        public void PublishMessageByFanout(string key, string routingKey, string msg)
        {
            try
            {
                if (_channel.IsClosed)
                {
                    Connect();
                }

                lock (_lockObj)
                {
                    var body = Encoding.UTF8.GetBytes(msg);
                    _channel.ExchangeDeclare(key, ExchangeType.Fanout);
                    _channel.BasicPublish(key, routingKey, null, body);
                }

                //Log.Info($"Sent message to MQ key:{key}");
            }
            catch (Exception e)
            {
                //Log.Error(e, $"{HostName} {e.StackTrace} {e.Message}");
            }

            //Cleaner.ExchangeDeclares.AddOrUpdate(key, DateTime.Now, (keya, oldValue) => DateTime.Now);
        }

        public void PublishMessageByFanout(string key, string routingKey, byte[] msg)
        {
            try
            {
                lock (_lockObj)
                {
                    _channel.ExchangeDeclare(key, ExchangeType.Fanout);
                    _channel.BasicPublish(key, routingKey, null, msg);
                }

                //Log.Info($"Sent message to MQ key:{key}");
            }
            catch (Exception e)
            {
                //Log.Error(e, $"{HostName} {e.StackTrace} {e.Message}");
            }

            //Cleaner.ExchangeDeclares.AddOrUpdate(key, DateTime.Now, (keya, oldValue) => DateTime.Now);
        }

        public void PublishMessageByDirect(string key, string routingKey, string msg)
        {
            try
            {
                lock (_lockObj)
                {
                    var body = Encoding.UTF8.GetBytes(msg);
                    _channel.ExchangeDeclare(key, "direct");
                    _channel.BasicPublish(key, routingKey, null, body);
                }

                //Log.Info($"Sent message to MQ key:{key}");
            }
            catch (Exception e)
            {
                //Log.Error(e, $"{HostName} {e.StackTrace} {e.Message}");
            }
        }

        public void PublishMessageByTopic(string key, string routingKey, string msg, bool durable = false)
        {
            PublishMessageByTopic(key, routingKey, Encoding.UTF8.GetBytes(msg), durable);
        }

        public void PublishMessageByTopic(string key, string routingKey, byte[] msg, bool durable = false)
        {
            try
            {
                if (_channel.IsClosed)
                {
                    Connect();
                }

                lock (_lockObj)
                {
                    _channel.ExchangeDeclare(key, ExchangeType.Topic, durable);
                    _channel.BasicPublish(key, routingKey, null, msg);
                }
            }
            catch (Exception e)
            {
                //Log.Error(e, $"{HostName} {e.StackTrace} {e.Message}");
            }
        }

        public void PublishMessage(IModel channel, string key, string msg)
        {
            try
            {
                var body = Encoding.UTF8.GetBytes(msg);
                lock (_lockObj)
                {
                    channel.ExchangeDeclare(key, "fanout");
                    channel.BasicPublish(key, "", null, body);
                }

                //Log.Info($"Sent message to MQ key:{key}");
            }
            catch (Exception e)
            {
                // Log.Error(e, $"{e.StackTrace} {e.Message}");
            }
        }


        public void DeleteExChange(string key)
        {
            try
            {
                lock (_lockObj)
                {
                    _channel.ExchangeDelete(key);
                }

            }
            catch (Exception e)
            {
                //Log.Error(e, $"{e.StackTrace} {e.Message}");
            }
        }

        public void RegistConsumerByTopic(bool durable, string exchange, string routingKey,
            EventHandler<BasicDeliverEventArgs> eventHandler)
        {
            try
            {
                if (_channel.IsClosed)
                {
                    Connect();
                }

                lock (_lockObj)
                {
                    _channel.ExchangeDeclare(exchange, ExchangeType.Topic, durable);
                    var queueName = _channel.QueueDeclare().QueueName;
                    _channel.QueueBind(queueName, exchange, routingKey);

                    var consumer = new EventingBasicConsumer(_channel);
                    consumer.Received += eventHandler;
                    _channel.BasicConsume(queueName, true, consumer);
                    _topic.Add(new MqEnevt
                    {
                        Durable = durable,
                        Exchange = exchange,
                        RoutingKey = routingKey,
                        EventHandler = eventHandler
                    });
                }

            }
            catch (Exception e)
            {
                Nami.Delay(5).Seconds()
                    .Do(() => { RegistConsumerByTopic(durable, exchange, routingKey, eventHandler); });
            }
        }

        public void DeleteExChange(IModel channel, string key)
        {
            try
            {
                lock (_lockObj)
                {
                    channel.ExchangeDelete(key);
                }

            }
            catch (Exception e)
            {
                // Log.Error(e, $"{e.StackTrace} {e.Message}");
            }
        }

        public void Close()
        {
            try
            {
                lock (_lockObj)
                {
                    SelfClose = true;
                    _channel.Close();
                    _mqConnection.Close();
                }
            }
            catch (Exception e)
            {
                //Log.Error(e, $"{e.StackTrace} {e.Message}");
            }
        }

        private class MqEnevt
        {
            public string Exchange { get; set; }
            public string RoutingKey { get; set; }

            /// <summary>
            /// 是否持久
            /// </summary>
            public bool Durable { get; set; }

            public EventHandler<BasicDeliverEventArgs> EventHandler { get; set; }
        }
    }
}
