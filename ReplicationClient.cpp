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

        CString GetISOTime(long int Delta = 0) {
            CString S;
            TCHAR buffer[80] = {0};

            struct timeval now = {};
            struct tm *timeinfo = nullptr;

            gettimeofday(&now, nullptr);
            if (Delta > 0) now.tv_sec += Delta;
            timeinfo = gmtime(&now.tv_sec);

            strftime(buffer, sizeof buffer, "%FT%T", timeinfo);

            S.Format("%s.%03ldZ", buffer, now.tv_usec / 1000);

            return S;
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationMessageHandler --------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationMessageHandler::CReplicationMessageHandler(CReplicationMessageHandlerManager *AManager,
                const CWSMessage &Message, COnMessageHandlerEvent &&Handler): CCollectionItem(AManager), m_Handler(Handler) {
            m_Message = Message;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationMessageHandler::Handler(CWebSocketConnection *AConnection) {
            if (m_Handler != nullptr) {
                m_Handler(this, AConnection);
            }
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationMessageHandlerManager -------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationMessageHandler *CReplicationMessageHandlerManager::Get(int Index) const {
            return dynamic_cast<CReplicationMessageHandler *> (inherited::GetItem(Index));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationMessageHandlerManager::Set(int Index, CReplicationMessageHandler *Value) {
            inherited::SetItem(Index, Value);
        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationMessageHandler *CReplicationMessageHandlerManager::Add(CWebSocketConnection *AConnection,
                const CWSMessage &Message, COnMessageHandlerEvent &&Handler, uint32_t Key) {

            if (AConnection->Connected() && !AConnection->ClosedGracefully()) {
                auto pHandler = new CReplicationMessageHandler(this, Message, static_cast<COnMessageHandlerEvent &&> (Handler));

                CString sResult;
                CWSProtocol::Response(Message, sResult);

                auto pWSReply = AConnection->WSReply();

                pWSReply->Clear();
                pWSReply->SetPayload(sResult, Key);

                AConnection->SendWebSocket(true);

                return pHandler;
            }

            return nullptr;
        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationMessageHandler *CReplicationMessageHandlerManager::FindMessageById(const CString &Value) const {
            CReplicationMessageHandler *pHandler;

            for (int i = 0; i < Count(); ++i) {
                pHandler = Get(i);
                if (pHandler->Message().UniqueId == Value)
                    return pHandler;
            }

            return nullptr;
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CCustomReplicationClient ----------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CCustomReplicationClient::CCustomReplicationClient(): CHTTPClient(), CGlobalComponent() {
            m_pConnection = nullptr;
            m_pTimer = nullptr;
            m_Key = GetUID(16);
            m_ErrorCount = -1;
            m_TimerInterval = 0;
            m_PingDateTime = 0;
            m_PongDateTime = 0;
            m_HeartbeatDateTime = 0;
            m_RegistrationDateTime = 0;
            m_HeartbeatInterval = 600;
            m_Authorized = false;
            m_UpdateConnected = false;
            m_OnMessage = nullptr;
            m_OnError = nullptr;
            m_OnWebSocketError = nullptr;
            m_OnProtocolError = nullptr;
        }
        //--------------------------------------------------------------------------------------------------------------

        CCustomReplicationClient::CCustomReplicationClient(const CLocation &URI): CHTTPClient(URI.hostname, URI.port), CGlobalComponent(), m_URI(URI) {
            m_pConnection = nullptr;
            m_pTimer = nullptr;
            m_Key = GetUID(16);
            m_ErrorCount = -1;
            m_TimerInterval = 0;
            m_PingDateTime = 0;
            m_PongDateTime = 0;
            m_HeartbeatDateTime = 0;
            m_RegistrationDateTime = 0;
            m_HeartbeatInterval = 600;
            m_Authorized = false;
            m_UpdateConnected = false;
            m_OnMessage = nullptr;
            m_OnError = nullptr;
            m_OnWebSocketError = nullptr;
            m_OnProtocolError = nullptr;
        }
        //--------------------------------------------------------------------------------------------------------------

        CCustomReplicationClient::~CCustomReplicationClient() {
            SwitchConnection(nullptr);
            delete m_pTimer;
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CCustomReplicationClient::Connected() const {
            if (Assigned(m_pConnection)) {
                return m_pConnection->Connected();
            }
            return false;
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CCustomReplicationClient::Authorized() const {
            return m_Authorized;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::AddToConnection(CWebSocketConnection *AConnection) {
            if (Assigned(AConnection)) {
                int Index = AConnection->Data().IndexOfName(REPLICATION_CONNECTION_DATA_NAME);
                if (Index == -1) {
                    AConnection->Data().AddObject(REPLICATION_CONNECTION_DATA_NAME, this);
                } else {
                    delete AConnection->Data().Objects(Index);
                    AConnection->Data().Objects(Index, this);
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DeleteFromConnection(CWebSocketConnection *AConnection) {
            if (Assigned(AConnection)) {
                int Index = AConnection->Data().IndexOfObject(this);
                if (Index != -1)
                    AConnection->Data().Delete(Index);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        CCustomReplicationClient *CCustomReplicationClient::FindOfConnection(CWebSocketConnection *AConnection) {
            int Index = AConnection->Data().IndexOfName(REPLICATION_CONNECTION_DATA_NAME);
            if (Index == -1)
                throw Delphi::Exception::ExceptionFrm("Not found charging point in connection");

            auto Object = AConnection->Data().Objects(Index);
            if (Object == nullptr)
                throw Delphi::Exception::ExceptionFrm("Object in connection data is null");

            return dynamic_cast<CCustomReplicationClient *> (Object);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SwitchConnection(CReplicationConnection *AConnection) {
            if (m_pConnection != AConnection) {
                BeginUpdate();

                if (Assigned(m_pConnection)) {
                    DeleteFromConnection(m_pConnection);
                    m_pConnection->Disconnect();
                    if (AConnection == nullptr)
                        delete m_pConnection;
                }

                if (Assigned(AConnection)) {
                    AddToConnection(AConnection);
                }

                m_pConnection = AConnection;

                EndUpdate();
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SetUpdateConnected(bool Value) {
            if (Value != m_UpdateConnected) {
                m_UpdateConnected = Value;
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::IncErrorCount() {
            if (m_ErrorCount == UINT32_MAX)
                m_ErrorCount = 0;
            m_ErrorCount++;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SetTimerInterval(int Value) {
            if (m_TimerInterval != Value) {
                m_TimerInterval = Value;
                UpdateTimer();
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::UpdateTimer() {
            if (m_pTimer == nullptr) {
                m_pTimer = CEPollTimer::CreateTimer(CLOCK_MONOTONIC, TFD_NONBLOCK);
                m_pTimer->AllocateTimer(m_pEventHandlers, m_TimerInterval, m_TimerInterval);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                m_pTimer->OnTimer([this](auto && AHandler) { DoTimer(AHandler); });
#else
                m_pTimer->OnTimer(std::bind(&CCustomReplicationClient::DoTimer, this, _1));
#endif
            } else {
                m_pTimer->SetTimer(m_TimerInterval, m_TimerInterval);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoTimer(CPollEventHandler *AHandler) {
            uint64_t exp;

            auto pTimer = dynamic_cast<CEPollTimer *> (AHandler->Binding());
            pTimer->Read(&exp, sizeof(uint64_t));

            Heartbeat(AHandler->TimeStamp());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoDebugWait(CObject *Sender) {
            auto pConnection = dynamic_cast<CReplicationConnection *> (Sender);
            if (Assigned(pConnection))
                DebugRequest(pConnection->Request());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoDebugRequest(CObject *Sender) {
            auto pConnection = dynamic_cast<CReplicationConnection *> (Sender);
            if (Assigned(pConnection)) {
                if (pConnection->Protocol() == pHTTP) {
                    DebugRequest(pConnection->Request());
                } else {
                    WSDebug(pConnection->WSRequest());
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoDebugReply(CObject *Sender) {
            auto pConnection = dynamic_cast<CReplicationConnection *> (Sender);
            if (Assigned(pConnection)) {
                if (pConnection->Protocol() == pHTTP) {
                    DebugReply(pConnection->Reply());
                } else {
                    WSDebug(pConnection->WSReply());
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoPing(CObject *Sender) {

        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoPong(CObject *Sender) {
            m_PongDateTime = Now();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoConnectStart(CIOHandlerSocket *AIOHandler, CPollEventHandler *AHandler) {
            auto pConnection = new CReplicationConnection(this);
            pConnection->IOHandler(AIOHandler);
            pConnection->AutoFree(false);
            AHandler->Binding(pConnection);
            SwitchConnection(pConnection);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoConnect(CPollEventHandler *AHandler) {
            auto pConnection = dynamic_cast<CReplicationConnection *> (AHandler->Binding());

            if (pConnection == nullptr) {
                AHandler->Stop();
                return;
            }

            try {
                auto pIOHandler = (CIOHandlerSocket *) pConnection->IOHandler();

                if (pIOHandler->Binding()->CheckConnection()) {
                    ClearErrorCount();
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                    pConnection->OnDisconnected([this](auto &&Sender) { DoDisconnected(Sender); });
                    pConnection->OnWaitRequest([this](auto &&Sender) { DoDebugWait(Sender); });
                    pConnection->OnRequest([this](auto &&Sender) { DoDebugRequest(Sender); });
                    pConnection->OnReply([this](auto &&Sender) { DoDebugReply(Sender); });
                    pConnection->OnPing([this](auto &&Sender) { DoPing(Sender); });
                    pConnection->OnPong([this](auto &&Sender) { DoPong(Sender); });
#else
                    pConnection->OnDisconnected(std::bind(&CReplicationClient::DoDisconnected, this, _1));
                    pConnection->OnWaitRequest(std::bind(&CReplicationClient::DoDebugWait, this, _1));
                    pConnection->OnRequest(std::bind(&CReplicationClient::DoDebugRequest, this, _1));
                    pConnection->OnReply(std::bind(&CReplicationClient::DoDebugReply, this, _1));
                    pConnection->OnPing(std::bind(&CReplicationClient::DoPing, this, _1));
                    pConnection->OnPong(std::bind(&CReplicationClient::DoPong, this, _1));
#endif
                    AHandler->Start(etIO);

                    pConnection->Session() = m_Session;
                    pConnection->URI() = m_URI;

                    DoConnected(pConnection);
                    Handshake(pConnection);
                }
            } catch (Delphi::Exception::Exception &E) {
                IncErrorCount();
                AHandler->Stop();
                SwitchConnection(nullptr);
                throw ESocketError(E.ErrorCode(), "Connection failed ");
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoWebSocket(CHTTPClientConnection *AConnection) {

            auto pWSRequest = AConnection->WSRequest();
            auto pWSReply = AConnection->WSReply();

            pWSReply->Clear();

            CWSMessage jmRequest;
            CWSMessage jmResponse;

            const CString csRequest(pWSRequest->Payload());

            CWSProtocol::Request(csRequest, jmRequest);

            try {
                if (jmRequest.MessageTypeId == WSProtocol::mtCall) {
                    int i;
                    CReplicationClientActionHandler *pHandler;
                    for (i = 0; i < m_Actions.Count(); ++i) {
                        pHandler = (CReplicationClientActionHandler *) m_Actions.Objects(i);
                        if (pHandler->Allow()) {
                            const auto &action = m_Actions.Strings(i);
                            if (action == jmRequest.Action) {
                                CWSProtocol::PrepareResponse(jmRequest, jmResponse);
                                DoMessage(jmRequest);
                                pHandler->Handler(this, jmRequest, jmResponse);
                                break;
                            }
                        }
                    }

                    if (i == m_Actions.Count()) {
                        SendNotSupported(jmRequest.UniqueId, CString().Format("Action %s not supported.", jmRequest.Action.c_str()));
                    }
                } else {
                    auto pHandler = m_Messages.FindMessageById(jmRequest.UniqueId);
                    if (Assigned(pHandler)) {
                        jmRequest.Action = pHandler->Message().Action;
                        DoMessage(jmRequest);
                        pHandler->Handler(AConnection);
                        delete pHandler;
                    }
                }
            } catch (std::exception &e) {
                Log()->Error(APP_LOG_ERR, 0, e.what());
                SendInternalError(jmRequest.UniqueId, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoHTTP(CHTTPClientConnection *AConnection) {
            auto pReply = AConnection->Reply();

            if (pReply->Status == CHTTPReply::switching_protocols) {
#ifdef _DEBUG
                WSDebugConnection(AConnection);
#endif
                AConnection->SwitchingProtocols(pWebSocket);
            } else {
                DoWebSocketError(AConnection);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CCustomReplicationClient::DoExecute(CTCPConnection *AConnection) {
            auto pConnection = dynamic_cast<CHTTPClientConnection *> (AConnection);
            if (pConnection->Protocol() == pWebSocket) {
                DoWebSocket(pConnection);
            } else {
                DoHTTP(pConnection);
            }
            return true;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::Initialize() {
            SetTimerInterval(1000);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SetURI(const CLocation &URI) {
            m_URI = URI;
            m_Host = URI.hostname;
            m_Port = URI.port;
            m_UsedSSL = m_Port == HTTP_SSL_PORT;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::Handshake(CReplicationConnection *AConnection) {
            auto pRequest = AConnection->Request();

            pRequest->AddHeader("Sec-WebSocket-Version", "13");
            pRequest->AddHeader("Sec-WebSocket-Key", base64_encode(m_Key));
            pRequest->AddHeader("Upgrade", "websocket");

            CHTTPRequest::Prepare(pRequest, _T("GET"), m_URI.href().c_str(), nullptr, "Upgrade");

            AConnection->SendRequest(true);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::Ping() {
            if (Assigned(m_pConnection)) {
                if (m_pConnection->Connected()) {
                    m_pConnection->SendWebSocketPing(true);
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SendMessage(const CWSMessage &Message, bool ASendNow) {
            CString sResponse;
            DoMessage(Message);
            CWSProtocol::Response(Message, sResponse);
            chASSERT(m_pConnection);
            if (m_pConnection != nullptr && m_pConnection->Connected()) {
                m_pConnection->WSReply()->SetPayload(sResponse, (uint32_t) MsEpoch());
                m_pConnection->SendWebSocket(ASendNow);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        CWSMessage CCustomReplicationClient::RequestToMessage(CWebSocketConnection *AWSConnection) {
            auto pWSRequest = AWSConnection->WSRequest();
            CWSMessage Message;
            const CString Payload(pWSRequest->Payload());
            CWSProtocol::Request(Payload, Message);
            AWSConnection->ConnectionStatus(csReplySent);
            pWSRequest->Clear();
            return Message;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SendMessage(const CWSMessage &Message, COnMessageHandlerEvent &&Handler) {
            m_Messages.Add(m_pConnection, Message, static_cast<COnMessageHandlerEvent &&> (Handler), (uint32_t) MsEpoch());
            DoMessage(Message);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SendNotSupported(const CString &UniqueId, const CString &ErrorDescription, const CJSON &Payload) {
            SendMessage(CWSProtocol::CallError(UniqueId, 404, ErrorDescription, Payload));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SendProtocolError(const CString &UniqueId, const CString &ErrorDescription, const CJSON &Payload) {
            SendMessage(CWSProtocol::CallError(UniqueId, 400, ErrorDescription, Payload));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::SendInternalError(const CString &UniqueId, const CString &ErrorDescription, const CJSON &Payload) {
            SendMessage(CWSProtocol::CallError(UniqueId, 500, ErrorDescription, Payload));
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoMessage(const CWSMessage &Message) {
            if (m_OnMessage != nullptr) {
                m_OnMessage(this, Message);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoError(int Code, const CString &Message) {
            if (m_OnError != nullptr) {
                m_OnError(this, Code, Message);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CCustomReplicationClient::DoWebSocketError(CTCPConnection *AConnection) {
            if (m_OnWebSocketError != nullptr)
                m_OnWebSocketError(AConnection);
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationClient ----------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationClient::CReplicationClient(): CCustomReplicationClient() {
            m_SendCount = 0;
            m_ApplyDate = 0;
            m_Proxy = false;
            m_OnHeartbeat = nullptr;
            m_OnTimeOut = nullptr;
            m_OnReplicationLog = nullptr;
            m_OnCheckReplicationLog = nullptr;
        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationClient::CReplicationClient(const CLocation &URI): CCustomReplicationClient(URI) {
            m_SendCount = 0;
            m_ApplyDate = 0;
            m_Proxy = false;
            m_OnHeartbeat = nullptr;
            m_OnTimeOut = nullptr;
            m_OnReplicationLog = nullptr;
            m_OnCheckReplicationLog = nullptr;
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

        void CReplicationClient::SendAuthorize() {

            auto OnRequest = [this](CReplicationMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
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

            auto OnRequest = [this](CReplicationMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
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

            auto OnRequest = [this](CReplicationMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
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

        void CReplicationClient::SendGetMaxRelay() {

            auto OnRequest = [this](CReplicationMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
                const auto &wsMessage = RequestToMessage(AWSConnection);
                if (wsMessage.MessageTypeId == mtCallResult) {
                    if (wsMessage.Payload.HasOwnProperty("id")) {
                        const auto &caId = wsMessage.Payload["id"];
                        if (caId.AsString() != "null") {
                            DoCheckReplicationLog(caId.AsLong());
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

            auto OnRequest = [this](CReplicationMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
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

            auto OnRequest = [this](CReplicationMessageHandler *AHandler, CWebSocketConnection *AWSConnection) {
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
            Message.Payload = CString().Format(R"({"id": %d, "source": "%s"})", RelayId, m_Source.c_str());

            SendMessage(Message, OnRequest);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::PushData() {
            for (int i = 0; i < Data().Count(); ++i) {
                SendData(Data()[i]);
            }
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
            if (Active() && Connected() && !Connection()->ClosedGracefully() && Connection()->Protocol() == pWebSocket) {
                if (m_PongDateTime == 0)
                    m_PongDateTime = Now;

                if (Now - m_PongDateTime >= (CDateTime) 90 / SecsPerDay) {
                    DoTimeOut();
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
                    }

                    if (Now >= m_HeartbeatDateTime) {
                        m_HeartbeatDateTime = Now + (CDateTime) m_HeartbeatInterval / SecsPerDay;
                        DoHeartbeat();
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoReplicationLog(const CJSON &Payload) {
            if (m_OnReplicationLog != nullptr) {
                m_OnReplicationLog(this, Payload);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoCheckReplicationLog(unsigned long RelayId) {
            if (m_OnCheckReplicationLog != nullptr) {
                m_OnCheckReplicationLog(this, RelayId);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoHeartbeat() {
            if (m_OnHeartbeat != nullptr) {
                m_OnHeartbeat(this);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationClient::DoTimeOut() {
            if (m_OnTimeOut != nullptr) {
                m_OnTimeOut(this);
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
