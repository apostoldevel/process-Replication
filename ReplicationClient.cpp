/*++

Program name:

 Апостол CRM

Module Name:

  ReplicationClient.cpp

Notices:

  Replication Client

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#include "Core.hpp"
#include "ReplicationClient.hpp"
//----------------------------------------------------------------------------------------------------------------------

#define REPLICATION_CONNECTION_DATA_NAME "Replication"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Replication {

        CString GenUniqueId() {
            return GetUID(32).Lower();
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationClient ----------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationClient::CReplicationClient(): CCustomWebSocketClient() {
            m_SendCount = 0;
            m_ApplyDate = 0;
            m_MaxLogId = 0;
            m_MaxRelayId = 0;
            m_PingDateTime = 0;
            m_PongDateTime = 0;
            m_HeartbeatDateTime = 0;
            m_RegistrationDateTime = 0;
            m_HeartbeatInterval = 600;
            m_Proxy = false;
            m_Authorized = false;
            m_OnReplicationLog = nullptr;
            m_OnReplicationCheckLog = nullptr;
            m_OnReplicationCheckRelay = nullptr;
        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationClient::CReplicationClient(const CLocation &URI): CCustomWebSocketClient(URI) {
            m_SendCount = 0;
            m_ApplyDate = 0;
            m_MaxLogId = 0;
            m_MaxRelayId = 0;
            m_PingDateTime = 0;
            m_PongDateTime = 0;
            m_HeartbeatDateTime = 0;
            m_RegistrationDateTime = 0;
            m_HeartbeatInterval = 600;
            m_Proxy = false;
            m_Authorized = false;
            m_OnReplicationLog = nullptr;
            m_OnReplicationCheckLog = nullptr;
            m_OnReplicationCheckRelay = nullptr;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::Reload() {
            m_Authorized = false;
            m_SendCount = 0;
            m_ApplyDate = 0;
            m_PongDateTime = 0;
            m_HeartbeatDateTime = 0;
            m_RegistrationDateTime = 0;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::CheckCallError(const CWSMessage &Error, const CWSMessage &Message) {
            if (Error.ErrorCode == 401) {
                m_Authorized = false;
                m_RegistrationDateTime = 0;
                m_MessageList.Add(Message);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CReplicationClient::Authorized() const {
            return m_Authorized;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::SendAuthorize() {

            auto OnRequest = [this](CWebSocketMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
                const auto &wsMessage = RequestToMessage(AWSConnection);
                if (wsMessage.MessageTypeId == mtCallResult) {
                    if (wsMessage.Payload.HasOwnProperty("authorized")) {
                        m_Authorized = wsMessage.Payload["authorized"].AsBoolean();
                    }
                    SendSubscribe();
                } else if (wsMessage.MessageTypeId == mtCallError) {
                    DoError(wsMessage.ErrorCode, wsMessage.ErrorMessage);
                }
            };

            CWSMessage Message;

            Message.MessageTypeId = WSProtocol::mtOpen;
            Message.UniqueId = GenUniqueId();
            Message.Action = "Authorize";
            Message.Payload = CString().Format(R"({"secret": "%s"})", m_Secret.c_str());

            SendMessage(Message, OnRequest);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::SendSubscribe() {

            auto OnRequest = [this](CWebSocketMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
                const auto &wsMessage = RequestToMessage(AWSConnection);
                if (wsMessage.MessageTypeId == mtCallResult) {
                    SendGetMaxRelay();
                } else if (wsMessage.MessageTypeId == mtCallError) {
                    DoError(wsMessage.ErrorCode, wsMessage.ErrorMessage);
                }
            };

            CWSMessage Message;

            Message.MessageTypeId = WSProtocol::mtCall;
            Message.UniqueId = GenUniqueId();
            Message.Action = "/observer/subscribe";
            Message.Payload = CString().Format(R"({"publisher": "replication", "params": {"source": "%s"}})", m_Source.c_str());

            SendMessage(Message, OnRequest);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::SendApply() {

            auto OnRequest = [this](CWebSocketMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
                const auto &wsMessage = RequestToMessage(AWSConnection);
                if (wsMessage.MessageTypeId == mtCallResult) {
                    if (wsMessage.Payload.HasOwnProperty("count")) {
                        const auto count = wsMessage.Payload["count"].AsInteger();
                        if (count > 0) {
                            m_ApplyDate = 0;
                        }
                    }
                } else if (wsMessage.MessageTypeId == mtCallError) {
                    CheckCallError(wsMessage, AHandler->Message());
                    DoError(wsMessage.ErrorCode, wsMessage.ErrorMessage);
                }
            };

            CWSMessage Message;

            Message.MessageTypeId = WSProtocol::mtCall;
            Message.UniqueId = GenUniqueId();
            Message.Action = "/replication/apply";
            Message.Payload = CString().Format(R"({"source": "%s"})", m_Source.c_str());

            SendMessage(Message, OnRequest);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::SendGetMaxLog() {

            auto OnRequest = [this](CWebSocketMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
                const auto &wsMessage = RequestToMessage(AWSConnection);
                if (wsMessage.MessageTypeId == mtCallResult) {
                    if (wsMessage.Payload.HasOwnProperty("id")) {
                        const auto &caId = wsMessage.Payload["id"];
                        if (caId.AsString() != "null") {
                            m_MaxLogId = caId.AsLong();
                            DoReplicationCheckLog(m_MaxLogId);
                        }
                    }
                } else if (wsMessage.MessageTypeId == mtCallError) {
                    DoError(wsMessage.ErrorCode, wsMessage.ErrorMessage);
                }
            };

            CWSMessage Message;

            Message.MessageTypeId = WSProtocol::mtCall;
            Message.UniqueId = GenUniqueId();
            Message.Action = "/replication/log/max";

            SendMessage(Message, OnRequest);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::SendGetMaxRelay() {

            auto OnRequest = [this](CWebSocketMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
                const auto &wsMessage = RequestToMessage(AWSConnection);
                if (wsMessage.MessageTypeId == mtCallResult) {
                    if (wsMessage.Payload.HasOwnProperty("id")) {
                        const auto &caId = wsMessage.Payload["id"];
                        if (caId.AsString() != "null") {
                            m_MaxRelayId = caId.AsLong();
                            DoReplicationCheckRelay(m_MaxRelayId);
                        }
                    }

                    PushData();
                    PushMessageList();
                } else if (wsMessage.MessageTypeId == mtCallError) {
                    DoError(wsMessage.ErrorCode, wsMessage.ErrorMessage);
                }
            };

            CWSMessage Message;

            Message.MessageTypeId = WSProtocol::mtCall;
            Message.UniqueId = GenUniqueId();
            Message.Action = "/replication/relay/max";
            Message.Payload = CString().Format(R"({"source": "%s"})", m_Source.c_str());

            SendMessage(Message, OnRequest);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::SendData(const CString &Data) {

            auto OnRequest = [this](CWebSocketMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
                const auto &wsMessage = RequestToMessage(AWSConnection);
                if (wsMessage.MessageTypeId == mtCallError) {
                    CheckCallError(wsMessage, AHandler->Message());
                    DoError(wsMessage.ErrorCode, wsMessage.ErrorMessage);
                }

                m_SendCount--;
                if (m_SendCount == 0) {
                    m_ApplyDate = 0;
                } else {
                    m_ApplyDate = Now() + (CDateTime) 5 / SecsPerDay;
                }
            };

            CWSMessage Message;

            Message.MessageTypeId = WSProtocol::mtCall;
            Message.UniqueId = GenUniqueId();
            Message.Action = "/replication/relay/add";
            Message.Payload = Data;

            const CString Source("source");
            const CString Proxy("proxy");

            if (Message.Payload.Object().HasOwnProperty(Source)) {
                Message.Payload.Object()[Source] = m_Source;
            } else {
                Message.Payload.Object().AddPair(Source, m_Source);
            }

            if (Message.Payload.Object().HasOwnProperty(Proxy)) {
                Message.Payload.Object()[Proxy] = m_Proxy;
            } else {
                Message.Payload.Object().AddPair(Proxy, m_Proxy);
            }

            m_SendCount++;
            SendMessage(Message, OnRequest);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::Replication(size_t RelayId) {

            auto OnRequest = [this](CWebSocketMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
                const auto &wsMessage = RequestToMessage(AWSConnection);
                if (wsMessage.MessageTypeId == mtCallResult) {
                    DoReplicationLog(wsMessage.Payload);
                } else if (wsMessage.MessageTypeId == mtCallError) {
                    CheckCallError(wsMessage, AHandler->Message());
                    DoError(wsMessage.ErrorCode, wsMessage.ErrorMessage);
                }
            };

            CWSMessage Message;

            Message.MessageTypeId = WSProtocol::mtCall;
            Message.UniqueId = GenUniqueId();
            Message.Action = "/replication/log";
            Message.Payload = CString().Format(R"({"id": %d, "source": "%s", "reclimit": 1})", RelayId, m_Source.c_str());

            SendMessage(Message, OnRequest);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::PushData() {
            for (int i = 0; i < Data().Count(); ++i) {
                SendData(Data()[i]);
            }
            Data().Clear();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::PushMessageList() {
            for (int i = 0; i < m_MessageList.Count(); ++i) {
                SendMessage(m_MessageList[i], true);
            }
            m_MessageList.Clear();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::Heartbeat(CDateTime Now) {
            if (Connection() != nullptr && (Connection()->Protocol() == pWebSocket && Active()) && Connected()) {
                if (m_PongDateTime == 0)
                    m_PongDateTime = Now;

                if (Now - m_PongDateTime >= (CDateTime) 90 / SecsPerDay) {
                    DoTimeOut();
                    SwitchConnection(nullptr);
                    Reload();
                    return;
                }

                if (Now >= m_PingDateTime) {
                    m_PingDateTime = Now + (CDateTime) 60 / SecsPerDay; // 60 sec
                    Ping();
                } else if (!m_Authorized) {
                    if (Now >= m_RegistrationDateTime) {
                        m_RegistrationDateTime = Now + (CDateTime) 30 / SecsPerDay; // 30 sec
                        SendAuthorize();
                    }
                } else {
                    if (Now >= m_ApplyDate) {
                        m_ApplyDate = Now + (CDateTime) 60 / MinsPerDay;

                        SendApply();
                        SendGetMaxLog();
                        SendGetMaxRelay();
                    }

                    if (Now >= m_HeartbeatDateTime) {
                        m_HeartbeatDateTime = Now + (CDateTime) m_HeartbeatInterval / SecsPerDay;
                        DoHeartbeat();
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoPing(CObject *Sender) {

        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoPong(CObject *Sender) {
            m_PongDateTime = Now();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoReplicationLog(const CJSON &Payload) {
            if (m_OnReplicationLog != nullptr) {
                m_OnReplicationLog(this, Payload);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoReplicationCheckLog(unsigned long Id) {
            if (m_OnReplicationCheckLog != nullptr) {
                m_OnReplicationCheckLog(this, Id);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoReplicationCheckRelay(unsigned long RelayId) {
            if (m_OnReplicationCheckRelay != nullptr) {
                m_OnReplicationCheckRelay(this, RelayId);
            }
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationClientItem ------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationClientItem::CReplicationClientItem(CReplicationClientManager *AManager): CCollectionItem(AManager), CReplicationClient() {

        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationClientItem::CReplicationClientItem(CReplicationClientManager *AManager, const CLocation &URI):
                CCollectionItem(AManager), CReplicationClient(URI) {

        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationClientManager ---------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationClientItem *CReplicationClientManager::GetItem(int Index) const {
            return (CReplicationClientItem *) inherited::GetItem(Index);
        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationClientItem *CReplicationClientManager::Add(const CLocation &URI) {
            return new CReplicationClientItem(this, URI);
        }

    }
}

}
