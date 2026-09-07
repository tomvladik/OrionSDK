using System;
using System.Threading;
using System.ServiceModel;
using System.ServiceModel.Channels;
using SolarWinds.InformationService.Contract2;
using FluentAssertions;
using SwqlStudio;
using Xunit;

namespace SwqlStudio.Tests
{
    public class ConnectionVisualTests
    {
        private static void RunInSta(Action action)
        {
            Exception ex = null;
            var thread = new Thread(() =>
            {
                try
                {
                    action();
                }
                catch (Exception e)
                {
                    ex = e;
                }
            });
            thread.SetApartmentState(ApartmentState.STA);
            thread.Start();
            thread.Join();

            if (ex != null)
                throw ex;
        }

        [Fact]
        public void QueryTab_ShowsDisconnected_OnConnectionClosed()
        {
            RunInSta(() =>
            {
                var tab = new QueryTab();
                var connection = new TestConnectionInfoWrapper("localhost", "user", "pass", "Orion (v3)");
                tab.ConnectionInfo = connection;

                // Simulate auto-disconnect event
                connection.TriggerClosed();
                System.Windows.Forms.Application.DoEvents(); // Process queued messages

                tab.CurrentStatus.Should().Be("Disconnected");
            });
        }

        [Fact]
        public void QueryTab_ShowsConnected_OnConnectionRestored()
        {
            RunInSta(() =>
            {
                var tab = new QueryTab();
                var connection = new TestConnectionInfoWrapper("localhost", "user", "pass", "Orion (v3)");
                tab.ConnectionInfo = connection;

                // Simulate disconnect then restore
                connection.TriggerClosed();
                System.Windows.Forms.Application.DoEvents(); // Process queued messages
                connection.TriggerRestored();
                System.Windows.Forms.Application.DoEvents(); // Process queued messages

                tab.CurrentStatus.Should().Be("Connected");
            });
        }

        [Fact]
        public void QueryTab_Resubscribes_WhenConnectionInfoChanges()
        {
            RunInSta(() =>
            {
                var tab = new QueryTab();
                var c1 = new TestConnectionInfoWrapper("server1", "user", "pass", "Orion (v3)");
                var c2 = new TestConnectionInfoWrapper("server2", "user", "pass", "Orion (v3)");

                tab.ConnectionInfo = c1;
                c1.TriggerClosed();
                System.Windows.Forms.Application.DoEvents(); // Process queued messages
                tab.CurrentStatus.Should().Be("Disconnected");

                tab.ConnectionInfo = c2;
                c2.TriggerRestored();
                System.Windows.Forms.Application.DoEvents(); // Process queued messages
                tab.CurrentStatus.Should().Be("Connected");
            });
        }

        private class TestConnectionInfoWrapper : ConnectionInfo
        {
            public TestConnectionInfoWrapper(string server, string username, string password, string serverType)
                : base(server, username, password, serverType, new FakeInfoService())
            {
            }

            public void TriggerClosed() => OnConnectionLost();
            public void TriggerRestored() => OnConnectionRestored();
        }

        private class FakeInfoService : InfoServiceBase
        {
            private readonly string _serviceType;

            public FakeInfoService(string serviceType = "Orion (v3)")
            {
                _serviceType = serviceType;
                _binding = new CustomBinding();
                _credentials = new FakeServiceCredentials();
                _endpoint = string.Empty;
                _endpointConfigName = string.Empty;
            }

            public override string ServiceType => _serviceType;

            public override InfoServiceProxy CreateProxy(string server)
            {
                return null;
            }
        }

        private class FakeServiceCredentials : ServiceCredentials
        {
            public override CredentialType CredentialType => CredentialType.Username;

            public override void ApplyTo(ChannelFactory channelFactory)
            {
                // no-op
            }
        }
    }
}
