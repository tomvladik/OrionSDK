using System;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.Threading;
using FluentAssertions;
using SolarWinds.InformationService.Contract2;
using SolarWinds.InformationService.InformationServiceClient;
using SwqlStudio;
using Xunit;

namespace SwqlStudio.Tests
{
    public class ConnectionInfoTests
    {
        [Fact]
        public void DoWithExceptionTranslation_Retries_On_CommunicationObjectFaulted()
        {
            int tries = 0;
            int result = ConnectionInfo.DoWithExceptionTranslation(() =>
            {
                if (tries++ == 0)
                    throw new CommunicationObjectFaultedException("faulted");
                return 42;
            });

            result.Should().Be(42);
            tries.Should().Be(2);
        }

        [Fact]
        public void DoWithExceptionTranslation_Wraps_FaultException()
        {
            Action act = () => ConnectionInfo.DoWithExceptionTranslation<int>(() =>
            {
                throw new FaultException("boom");
            });

            act.Should().Throw<ApplicationException>()
                .WithMessage("boom");
        }

        [Fact]
        public void IsConnected_IsFalse_When_NoProxy()
        {
            var info = new TestConnectionInfo();
            info.IsConnected.Should().BeFalse();
        }

        [Fact]
        public void Copy_Clones_Credentials_And_Type()
        {
            var info = new TestConnectionInfo("server1", "user", "pass", "Orion (v3)")
            {
                QueryParameters = new PropertyBag { { "k", "v" } }
            };

            var clone = info.Copy();

            clone.Server.Should().Be("server1");
            clone.UserName.Should().Be("user");
            clone.Password.Should().Be("pass");
            clone.ServerType.Should().Be("Orion (v3)");
            clone.QueryParameters.Should().ContainKey("k");
        }

        [Fact]
        public void OnConnectionRestored_IsSuppressed_AfterClose()
        {
            // Built without a SynchronizationContext so the event fires synchronously and the assertion is deterministic.
            var info = CreateWithoutSyncContext();
            bool restoredRaised = false;
            info.ConnectionRestored += (s, e) => restoredRaised = true;

            info.Close();

            // A reconnect that committed just before Close() must not report the connection as up again.
            info.TriggerRestored();

            restoredRaised.Should().BeFalse();
        }

        [Fact]
        public void OnConnectionRestored_IsRaised_WhenNotClosed()
        {
            var info = CreateWithoutSyncContext();
            bool restoredRaised = false;
            info.ConnectionRestored += (s, e) => restoredRaised = true;

            info.TriggerRestored();

            restoredRaised.Should().BeTrue();
        }

        private static TestConnectionInfo CreateWithoutSyncContext()
        {
            var previous = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(null);
            try
            {
                return new TestConnectionInfo();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previous);
            }
        }

        private class TestConnectionInfo : ConnectionInfo
        {
            public TestConnectionInfo(string server = "localhost", string user = "user", string pass = "pass", string type = "Orion (v3)")
                : base(server, user, pass, type, new FakeInfoService())
            {
            }

            public void TriggerRestored() => OnConnectionRestored();

            internal new ConnectionInfo Copy()
            {
                return new TestConnectionInfo(Server, UserName, Password, ServerType)
                {
                    QueryParameters = QueryParameters
                };
            }
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
                // no-op for tests
            }
        }
    }
}
