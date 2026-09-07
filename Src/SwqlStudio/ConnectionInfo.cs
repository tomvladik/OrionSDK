using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Security;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using SolarWinds.InformationService.Contract2;
using SolarWinds.InformationService.Contract2.PubSub;
using SolarWinds.InformationService.InformationServiceClient;
using SolarWinds.Logging;
using SwqlStudio.Properties;
using SwqlStudio.Subscriptions;

namespace SwqlStudio
{
    public class ConnectionInfo : IDisposable
    {
        private static readonly Log log = new Log();

        private const int KeepAliveCheckIntervalSeconds = 10;
        private const int ReconnectRetryIntervalSeconds = 10;

        public string ServerType { get; set; }
        private string _server;
        private string _username;
        private string _password;

        private InfoServiceProxy _proxy;
        private readonly InfoServiceBase _infoServiceType;
        private CancellationTokenSource _keepAliveCts;
        private bool _connectionClosed;
        private readonly SynchronizationContext _syncContext;
        private readonly object _proxyLock = new object();

        public event EventHandler<EventArgs> ConnectionClosed;
        public event EventHandler<EventArgs> ConnectionClosing;
        public event EventHandler<EventArgs> ConnectionRestored;

        public ConnectionInfo(string server, string username, string password, string serverType)
            : this(server, username, password, serverType, InfoServiceFactory.Create(serverType, username, password))
        {
        }

        internal ConnectionInfo(string server, string username, string password, string serverType, InfoServiceBase infoServiceType)
        {
            ServerType = serverType;
            _server = server;
            _username = username;
            _password = password;

            _infoServiceType = infoServiceType;
            QueryParameters = new PropertyBag();
            _syncContext = SynchronizationContext.Current;
        }

        public Binding Binding
        {
            get { return _infoServiceType.Binding; }
        }

        public string Server
        {
            get { return _server; }
            set { _server = value; }
        }

        public string UserName
        {
            get { return _username; }
            set { _username = value; }
        }

        public string Password
        {
            get { return _password; }
            set { _password = value; }
        }

        public bool CanCreateSubscription { get; set; }

        public bool SupportsActiveSubscriptions
        {
            get { return _infoServiceType.SupportsActiveSubscriber; }
        }

        public string Title
        {
            get
            {
                return string.Format("{0} : {1}{2}", Server, ServerType, FormattedUserName);
            }
        }

        private string FormattedUserName
        {
            get
            {
                if (!string.IsNullOrEmpty(UserName))
                    return string.Format(" [{0}]", UserName);

                return UserName;
            }
        }

        public InfoServiceProxy Proxy
        {
            get { return _proxy; }
        }

        public InformationServiceConnection Connection { get; private set; }

        public static List<ServerType> AvailableServerTypes
        {
            get
            {
                List<ServerType> serverTypes = new List<ServerType>
                {
                    new ServerType { Type = "Orion (v3)", IsAuthenticationRequired = true },
                    new ServerType { Type = "Orion (v3) AD", IsAuthenticationRequired = false },
                    new ServerType { Type = "Orion (v3) Certificate", IsAuthenticationRequired = false },
                    new ServerType { Type = "Orion (v3) over HTTPS", IsAuthenticationRequired = true },
                    new ServerType { Type = "Orion (v3) over HTTPS legacy pre-2023", IsAuthenticationRequired = true}
                };

                if (Settings.Default.ShowCompressedModes)
                {
                    serverTypes.AddRange(new[]
                                        {
                                            new ServerType { Type = "Orion (v3) Compressed", IsAuthenticationRequired = true },
                                            new ServerType { Type = "Orion (v3) AD Compressed", IsAuthenticationRequired = false },
                                        });

                }

                return serverTypes;

            }
        }

        public PropertyBag QueryParameters { get; set; }

        public void Connect()
        {
            lock (_proxyLock)
            {
                if (_proxy == null || (_proxy != null && (_proxy.Channel.State == CommunicationState.Closed || _proxy.Channel.State == CommunicationState.Faulted)))
                {
                    if (_proxy != null)
                        _proxy.Dispose();

                    _proxy = _infoServiceType.CreateProxy(_server);
                    _proxy.OperationTimeout = TimeSpan.FromMinutes(Settings.Default.OperationTimeout);
                    _proxy.ChannelFactory.Endpoint.Behaviors.Add(new LogHeaderReaderBehavior());
                    _proxy.Open();
                    _connectionClosed = false;
                    StartKeepAlive();
                }

                Connection?.Dispose();
                Connection = new InformationServiceConnection((IInformationService)_proxy);
                Connection.Open();
            }
        }

        public bool IsConnected
        {
            get
            {
                lock (_proxyLock)
                {
                    return _proxy != null && _proxy.ClientChannel.State == CommunicationState.Opened;
                }
            }
        }

        internal NotificationDeliveryServiceProxy CreateActiveListenerProxy(INotificationSubscriber listener)
        {
            if (!_infoServiceType.SupportsActiveSubscriber)
                throw new InvalidOperationException("This connection type doesn't support active subscriptions");

            return _infoServiceType.CreateNotificationDeliveryServiceProxy(_server, listener);
        }

        private void StartKeepAlive()
        {
            StopKeepAlive();
            _keepAliveCts = new CancellationTokenSource();
            Task.Run(() => KeepAliveMonitor(_keepAliveCts.Token));
        }

        private void StopKeepAlive()
        {
            if (_keepAliveCts != null)
            {
                _keepAliveCts.Cancel();
                _keepAliveCts.Dispose();
                _keepAliveCts = null;
            }
        }

        private async Task KeepAliveMonitor(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(KeepAliveCheckIntervalSeconds), ct);

                    if (ct.IsCancellationRequested)
                        break;

                    if (!IsConnected)
                    {
                        OnConnectionLost();

                        while (!ct.IsCancellationRequested && !IsConnected)
                        {
                            if (await TryReconnectAsync(ct))
                            {
                                OnConnectionRestored();
                                break;
                            }

                            await Task.Delay(TimeSpan.FromSeconds(ReconnectRetryIntervalSeconds), ct);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when closing
            }
        }

        protected internal virtual void OnConnectionLost()
        {
            if (_connectionClosed)
                return;

            _connectionClosed = true;

            var handler = ConnectionClosed;
            if (handler != null)
            {
                if (_syncContext != null)
                    _syncContext.Post(_ => handler.Invoke(this, EventArgs.Empty), null);
                else
                    handler.Invoke(this, EventArgs.Empty);
            }
        }

        protected internal virtual void OnConnectionRestored()
        {
            _connectionClosed = false;

            var handler = ConnectionRestored;
            if (handler != null)
            {
                if (_syncContext != null)
                    _syncContext.Post(_ => handler.Invoke(this, EventArgs.Empty), null);
                else
                    handler.Invoke(this, EventArgs.Empty);
            }
        }

        private async Task<bool> TryReconnectAsync(CancellationToken ct)
        {
            try
            {
                if (ct.IsCancellationRequested)
                    return false;

                var newProxy = _infoServiceType.CreateProxy(_server);
                newProxy.OperationTimeout = TimeSpan.FromMinutes(Settings.Default.OperationTimeout);
                newProxy.ChannelFactory.Endpoint.Behaviors.Add(new LogHeaderReaderBehavior());
                newProxy.Open();

                lock (_proxyLock)
                {
                    _proxy?.Dispose();
                    _proxy = newProxy;

                    Connection?.Dispose();
                    Connection = new InformationServiceConnection((IInformationService)_proxy);
                    Connection.Open();
                }

                return true;
            }
            catch (Exception ex)
            {
                // Log the exception for diagnostics
                log.Error($"Reconnect attempt to {_server} failed", ex);
                return false;
            }
        }

        private void EnsureConnection()
        {
            if (!IsConnected)
                Connect();
        }

        public IEnumerable<T> Query<T>(string swql) where T : new()
        {
            EnsureConnection();

            using (var context = new InformationServiceContext(_proxy))
            using (var serviceQuery = context.CreateQuery<T>(swql))
            {
                var enumerator = serviceQuery.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    yield return enumerator.Current;
                }
            }
        }

        public DataTable Query(string swql)
        {
            XmlDocument dummy;
            XmlDocument dummy2;
            return Query(swql, out dummy, out dummy2);
        }

        public DataTable Query(string swql, out XmlDocument queryPlan, out XmlDocument queryStats)
        {
            XmlDocument tmpQueryPlan = null; // can't reference out parameter from closure
            XmlDocument tmpQueryStats = null; // can't reference out parameter from closure

            DataTable result = DoWithExceptionTranslation(
                delegate
                    {
                        EnsureConnection();

                        using (var context = new SwisSettingsContext { DataProviderTimeout = Settings.Default.DataProviderTimeout })
                        using (InformationServiceCommand command = new InformationServiceCommand(swql, Connection) { ApplicationTag = "SWQL Studio" })
                        {
                            foreach (var param in QueryParameters)
                                command.Parameters.AddWithValue(param.Key, param.Value);

                            InformationServiceDataAdapter dataAdapter = new InformationServiceDataAdapter(command);
                            DataTable resultDataTable = new DataTable();
                            dataAdapter.Fill(resultDataTable);

                            tmpQueryPlan = dataAdapter.QueryPlan;
                            tmpQueryStats = dataAdapter.QueryStats;
                            return resultDataTable;
                        }
                    });

            queryPlan = tmpQueryPlan;
            queryStats = tmpQueryStats;
            return result;
        }

        public static void DoWithExceptionTranslation(Action action)
        {
            DoWithExceptionTranslation(delegate
                                           {
                                               action();
                                               return 0;
                                           });
        }

        public static T DoWithExceptionTranslation<T>(Func<T> action, bool retryOnConnectionError = true)
        {
            string msg;
            Exception inner;
            bool couldRetry = false;

            try
            {
                return action();
            }
            catch (FaultException<InfoServiceFaultContract> ex)
            {
                msg = ex.Detail.Message;
                inner = ex;
            }
            catch (SecurityNegotiationException ex)
            {
                msg = ex.Message;
                inner = ex;
            }
            catch (FaultException ex)
            {
                msg = ex.InnerException?.Message ?? ex.Message;
                inner = ex.InnerException ?? ex;
            }
            catch (MessageSecurityException ex)
            {
                if (ex.InnerException is FaultException fault)
                {
                    msg = fault.Message;
                    inner = fault;
                    couldRetry = fault.Code?.SubCode?.Name == "BadContextToken";
                }
                else
                {
                    msg = ex.Message;
                    inner = ex;
                }
            }
            catch (CommunicationObjectFaultedException ex)
            {
                msg = ex.Message;
                inner = ex;
                couldRetry = true;
            }
            catch (Exception ex)
            {
                msg = ex.Message;
                inner = ex;
            }

            if (couldRetry && retryOnConnectionError)
            {
                return DoWithExceptionTranslation(action, false);
            }

            throw new ApplicationException(msg, inner);
        }

        public XmlDocument QueryXml(string query, out XmlDocument queryPlan, out List<ErrorMessage> errorMessages, out XmlDocument queryStats)
        {
            EnsureConnection();
            Message results;
            errorMessages = null;

            using (new SwisSettingsContext { DataProviderTimeout = TimeSpan.FromSeconds(30), ApplicationTag = "SWQL Studio", AppendErrors = true })
            {
                results = _proxy.Query(new QueryXmlRequest(query, QueryParameters));
            }

            XmlReader reader = results.GetReaderAtBodyContents();
            var body = new XmlDocument(reader.NameTable);
            body.Load(reader);

            var nsmgr = new XmlNamespaceManager(reader.NameTable);
            nsmgr.AddNamespace("is", Constants.Namespace);

            bool hasErrors = false;
            if (results.Headers.FindHeader("hasErrors", Constants.Namespace) > -1)
            {
                hasErrors = results.Headers.GetHeader<bool>("hasErrors", Constants.Namespace);
            }

            if (hasErrors)
            {
                XmlNode errorsNode = body.SelectSingleNode("/is:QueryXmlResponse/is:QueryXmlResult/errors", nsmgr);

                if (errorsNode != null)
                {
                    errorMessages = new List<ErrorMessage>();

                    foreach (XmlNode node in errorsNode.ChildNodes)
                    {
                        XmlSerializer serializer = new XmlSerializer(typeof(ErrorMessage));
                        ErrorMessage message = (ErrorMessage)serializer.Deserialize(new StringReader(node.OuterXml));

                        if (message != null)
                            errorMessages.Add(message);
                    }

                    errorsNode.ParentNode.RemoveChild(errorsNode);
                }
            }

            // Extract query plan if present
            XmlNode queryPlanNode = body.SelectSingleNode("/is:QueryXmlResponse/is:QueryXmlResult/is:queryResult/is:queryPlan", nsmgr);
            if (queryPlanNode != null)
            {
                queryPlan = new XmlDocument();
                queryPlan.LoadXml(queryPlanNode.OuterXml);
                queryPlanNode.ParentNode.RemoveChild(queryPlanNode);
            }
            else
            {
                queryPlan = null;
            }

            queryStats = null;

            return body;
        }

        public void Dispose()
        {
            Close();
        }

        public void Close()
        {
            lock (_proxyLock)
            {
                if (_proxy != null)
                {
                    StopKeepAlive();

                    var listeners = ConnectionClosing;
                    listeners?.Invoke(this, EventArgs.Empty);

                    _proxy.Dispose();
                    _proxy = null;

                    _connectionClosed = true;
                    listeners = ConnectionClosed;
                    listeners?.Invoke(this, EventArgs.Empty);
                }
            }
        }

        internal ConnectionInfo Copy()
        {
            return new ConnectionInfo(_server, _username, _password, _infoServiceType.ServiceType)
            {
                QueryParameters = QueryParameters
            };
        }

        protected bool Equals(ConnectionInfo other)
        {
            return string.Equals(_server, other._server) && string.Equals(_username, other._username) && string.Equals(ServerType, other.ServerType);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ConnectionInfo)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = _server.GetHashCode();
                hashCode = (hashCode * 397) ^ _username.GetHashCode();
                hashCode = (hashCode * 397) ^ ServerType.GetHashCode();
                return hashCode;
            }
        }

        public override string ToString()
        {
            return Title;
        }
    }
}
