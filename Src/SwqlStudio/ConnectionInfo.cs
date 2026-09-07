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
        private Task _keepAliveTask;
        private bool _connectionClosed;
        private bool _isClosed;

        // Bumped whenever the proxy is replaced or closed, so a slow reconnect can detect it lost the race.
        private int _connectionGeneration;

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
                    _isClosed = false;
                    _connectionGeneration++;
                    StartKeepAlive();
                }

                Connection?.Dispose();
                Connection = new InformationServiceConnection((IInformationService)_proxy);
                Connection.Open();
            }
        }

        public virtual bool IsConnected
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

            var cts = new CancellationTokenSource();
            _keepAliveCts = cts;

            // Capture the token before scheduling; a concurrent Close() can null the field before the task starts.
            CancellationToken token = cts.Token;
            _keepAliveTask = Task.Run(() => KeepAliveMonitor(token));
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
                            int? committedGeneration = await TryReconnectAsync(ct);

                            if (committedGeneration.HasValue)
                            {
                                // Validated inside the state transition, so Close() cannot slip in behind the check.
                                RaiseConnectionRestored(committedGeneration.Value);

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
            catch (ObjectDisposedException)
            {
                // The token source was disposed by Close() while we were waiting on it.
            }
            catch (Exception ex)
            {
                // This runs detached, so an escaping exception would otherwise go unobserved.
                log.Error($"Keep-alive monitor for {_server} stopped unexpectedly", ex);
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

        private bool IsGenerationCurrent(int generation)
        {
            lock (_proxyLock)
            {
                return !_isClosed && _connectionGeneration == generation;
            }
        }

        protected internal virtual void OnConnectionRestored()
        {
            RaiseConnectionRestored(null);
        }

        /// <param name="expectedGeneration">Generation the caller committed, or null to accept whatever is current.</param>
        private void RaiseConnectionRestored(int? expectedGeneration)
        {
            EventHandler<EventArgs> handler;
            int generation;

            // The guard, the state change and the handler capture must be one atomic step,
            // otherwise Close() can slip in between them and we announce a closed connection as up.
            lock (_proxyLock)
            {
                if (_isClosed)
                    return;

                if (expectedGeneration.HasValue && _connectionGeneration != expectedGeneration.Value)
                    return;

                _connectionClosed = false;
                generation = _connectionGeneration;
                handler = ConnectionRestored;
            }

            if (handler == null)
                return;

            // Dispatch outside the lock; revalidate because Close() may run before this executes.
            if (_syncContext != null)
                _syncContext.Post(_ =>
                {
                    if (IsGenerationCurrent(generation))
                        handler.Invoke(this, EventArgs.Empty);
                }, null);
            else if (IsGenerationCurrent(generation))
                handler.Invoke(this, EventArgs.Empty);
        }

        /// <summary>Returns the generation it committed, or null when the attempt failed or lost the race.</summary>
        private async Task<int?> TryReconnectAsync(CancellationToken ct)
        {
            InfoServiceProxy newProxy = null;
            InformationServiceConnection newConnection = null;

            try
            {
                if (ct.IsCancellationRequested)
                    return null;

                int generationAtStart;
                lock (_proxyLock)
                {
                    if (_isClosed)
                        return null;

                    generationAtStart = _connectionGeneration;
                }

                // Build the whole replacement off to the side so a partial failure cannot corrupt live state.
                newProxy = _infoServiceType.CreateProxy(_server);
                newProxy.OperationTimeout = TimeSpan.FromMinutes(Settings.Default.OperationTimeout);
                newProxy.ChannelFactory.Endpoint.Behaviors.Add(new LogHeaderReaderBehavior());
                newProxy.Open();

                newConnection = new InformationServiceConnection((IInformationService)newProxy);
                newConnection.Open();

                lock (_proxyLock)
                {
                    // Close() or another reconnect may have won while we were opening; discard our replacement.
                    if (_isClosed || ct.IsCancellationRequested || _connectionGeneration != generationAtStart)
                        return null;

                    var oldProxy = _proxy;
                    var oldConnection = Connection;

                    _proxy = newProxy;
                    Connection = newConnection;
                    _connectionGeneration++;

                    newProxy = null;
                    newConnection = null;

                    DisposeQuietly(oldConnection);
                    DisposeQuietly(oldProxy);

                    return _connectionGeneration;
                }
            }
            catch (Exception ex)
            {
                log.Error($"Reconnect attempt to {_server} failed", ex);
                return null;
            }
            finally
            {
                // Non-null only when the attempt failed or lost the race.
                DisposeQuietly(newConnection);
                DisposeQuietly(newProxy);
            }
        }

        private static void DisposeQuietly(IDisposable disposable)
        {
            if (disposable == null)
                return;

            try
            {
                disposable.Dispose();
            }
            catch (Exception ex)
            {
                log.Warn("Failed to dispose a replaced connection resource.", ex);
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
                // Set before anything else so a reconnect already in flight refuses to commit.
                _isClosed = true;
                _connectionGeneration++;

                if (_proxy != null)
                {
                    StopKeepAlive();

                    var listeners = ConnectionClosing;
                    listeners?.Invoke(this, EventArgs.Empty);

                    DisposeQuietly(Connection);
                    Connection = null;

                    _proxy.Dispose();
                    _proxy = null;

                    _connectionClosed = true;
                    listeners = ConnectionClosed;
                    listeners?.Invoke(this, EventArgs.Empty);
                }
                else
                {
                    StopKeepAlive();
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
