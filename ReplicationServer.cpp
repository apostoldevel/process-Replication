/*++

Program name:

 Апостол CRM

Module Name:

  ReplicationServer.cpp

Notices:

  Replication Server

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#include "Core.hpp"
#include "ReplicationServer.hpp"
//----------------------------------------------------------------------------------------------------------------------

#include "jwt.h"
//----------------------------------------------------------------------------------------------------------------------

#define SERVICE_APPLICATION_NAME "service"
#define CONFIG_SECTION_NAME "process/Replication"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace BackEnd {

        namespace api {

            void replication_log(CStringList &SQL, unsigned long RelayId, unsigned int Limit = 150) {
                SQL.Add(CString().Format("SELECT row_to_json(r) FROM api.replication_log(%d, %d) AS r ORDER BY id DESC;", RelayId, Limit));
            }

            void get_replication_log(CStringList &SQL, unsigned long Id) {
                SQL.Add(CString().Format("SELECT row_to_json(r) FROM api.replication_log AS r WHERE id = %d;", Id));
            }

            void add_to_relay_log(CStringList &SQL, const CString &Source, unsigned long Id, const CString &DateTime, const CString &Action,
                    const CString &Schema, const CString &Name, const CString &Key, const CString &Data, const bool Proxy = false) {

                const auto &data = PQQuoteLiteral(Data);

                SQL.Add(CString()
                                .MaxFormatSize(256 + Source.Size() + DateTime.Size() + Action.Size() + Schema.Size() + Name.Size() + Key.Size() + data.Size())
                                .Format("SELECT * FROM api.add_to_relay_log(%s::text, %d::bigint, %s::timestamptz, %s::char, %s::text, %s::text, %s::jsonb, %s::jsonb, %s);",
                                        PQQuoteLiteral(Source).c_str(),
                                        Id,
                                        PQQuoteLiteral(DateTime).c_str(),
                                        PQQuoteLiteral(Action).c_str(),
                                        PQQuoteLiteral(Schema).c_str(),
                                        PQQuoteLiteral(Name).c_str(),
                                        PQQuoteLiteral(Key).c_str(),
                                        data.c_str(),
                                        Proxy ? "true" : "false"
                                ));
            }

            void get_max_relay_id(CStringList &SQL, const CString &Source) {
                SQL.Add(CString().Format("SELECT api.get_max_relay_id(%s);", PQQuoteLiteral(Source).c_str()));
            }

            void replication_apply(CStringList &SQL, const CString &Source) {
                SQL.Add(CString().Format("SELECT api.replication_apply(%s);", PQQuoteLiteral(Source).c_str()));
            }

            void replication_apply_relay(CStringList &SQL, const CString &Source, unsigned long Id) {
                SQL.Add(CString().Format("SELECT api.replication_apply_relay(%s, %d::bigint);", PQQuoteLiteral(Source).c_str(), Id));
            }
        }

    }

    namespace Replication {

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationHandler ---------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationHandler::CReplicationHandler(CReplicationServer *AModule, unsigned long ReplicationId,
                COnReplicationHandlerEvent && Handler): CPollConnection(AModule->ptrQueueManager()), m_Allow(true) {

            m_pModule = AModule;
            m_ReplicationId = ReplicationId;
            m_Handler = Handler;

            AddToQueue();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationHandler::Close() {
            m_Allow = false;
            RemoveFromQueue();
        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationHandler::~CReplicationHandler() {
            Close();
        }
        //--------------------------------------------------------------------------------------------------------------

        int CReplicationHandler::AddToQueue() {
            return m_pModule->AddToQueue(this);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationHandler::RemoveFromQueue() {
            m_pModule->RemoveFromQueue(this);
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CReplicationHandler::Handler() {
            if (m_Allow && m_Handler) {
                m_Handler(this);
                return true;
            }
            return false;
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationServer ----------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationServer::CReplicationServer(CCustomProcess *AParent, CApplication *AApplication):
                inherited(AParent, AApplication, ptCustom, "replication server") {

            m_CheckDate = 0;
            m_FixedDate = 0;
            m_ApplyDate = 0;
            m_ErrorCount = 0;

            m_Progress = 0;
            m_MaxQueue = Config()->PostgresPollMin();

            m_ApplyCount = 0;
            m_NeedCheckReplicationLog = false;

            m_Mode = rmSlave;
            m_Status = psStopped;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::BeforeRun() {
            sigset_t set;

            Application()->Header(Application()->Name() + ": replication server");

            Log()->Debug(APP_LOG_DEBUG_CORE, MSG_PROCESS_START, GetProcessName(), Application()->Header().c_str());

            InitSignals();

            Reload();

            SetUser(Config()->User(), Config()->Group());

            InitializePQClient(Application()->Title(), 1, Config()->PostgresPollMin());

            PQClientStart(_T("helper"));

            SigProcMask(SIG_UNBLOCK, SigAddSet(&set));

            SetTimerInterval(1000);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::AfterRun() {
            CApplicationProcess::AfterRun();
            PQClientStop();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::Run() {
            while (!sig_exiting) {

                Log()->Debug(APP_LOG_DEBUG_EVENT, _T("replication server cycle"));

                try {
                    PQClient().Wait();
                } catch (Delphi::Exception::Exception &E) {
                    Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
                }

                if (sig_terminate || sig_quit) {
                    if (sig_quit) {
                        sig_quit = 0;
                        Log()->Debug(APP_LOG_DEBUG_EVENT, _T("gracefully shutting down"));
                        Application()->Header(_T("replication server is shutting down"));
                    }

                    if (!sig_exiting) {
                        sig_exiting = 1;
                    }
                }

                if (sig_reconfigure) {
                    sig_reconfigure = 0;
                    Log()->Debug(APP_LOG_DEBUG_EVENT, _T("reconfiguring"));

                    Reload();
                }

                if (sig_reopen) {
                    sig_reopen = 0;
                    Log()->Debug(APP_LOG_DEBUG_EVENT, _T("reopening logs"));
                }
            }

            Log()->Debug(APP_LOG_DEBUG_EVENT, _T("stop replication server"));
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CReplicationServer::DoExecute(CTCPConnection *AConnection) {
            return CModuleProcess::DoExecute(AConnection);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::Reload() {
            CServerProcess::Reload();

            m_Providers.Clear();
            m_Tokens.Clear();

            Config()->IniFile().ReadSectionValues(CONFIG_SECTION_NAME, &m_Config);

            m_Mode = rmSlave;
            if (m_Config["mode"] == "proxy") {
                m_Mode = rmProxy;
            } else if (m_Config["mode"] == "master") {
                m_Mode = rmMaster;
            }

            m_Source = m_Config["source"];
            m_Server = m_Config["server"];

            m_Origin = m_Server;

            if (m_Source.IsEmpty()) {
                m_Source = CApostolModule::GetHostName();
            }

            const auto &provider = m_Config["provider"];
            const auto &application = m_Config["application"];
            const auto &oauth2 = m_Config["oauth2"];

            m_Tokens.AddPair(provider, CStringList());
            LoadOAuth2(oauth2, provider.empty() ? SYSTEM_PROVIDER_NAME : provider, application.empty() ? SERVICE_APPLICATION_NAME : application, m_Providers);

            m_ApplyCount = 0;
            m_ApplyDate = 0;
            m_CheckDate = 0;
            m_FixedDate = 0;
            m_ErrorCount = 0;

            m_Status = psStopped;
        }
        //--------------------------------------------------------------------------------------------------------------

        CString CReplicationServer::CreateToken(const CProvider &Provider, const CString &Application) {
            auto token = jwt::create()
                    .set_issuer(Provider.Issuer(Application))
                    .set_audience(Provider.ClientId(Application))
                    .set_issued_at(std::chrono::system_clock::now())
                    .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
                    .sign(jwt::algorithm::hs256{std::string(Provider.Secret(Application))});

            return token;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::FetchAccessToken(const CString &URI, const CString &Assertion,
                                                 COnSocketExecuteEvent &&OnDone, COnSocketExceptionEvent &&OnFailed) {

            auto OnRequest = [](CHTTPClient *Sender, CHTTPRequest *ARequest) {

                const auto &token_uri = Sender->Data()["token_uri"];
                const auto &grant_type = Sender->Data()["grant_type"];
                const auto &assertion = Sender->Data()["assertion"];

                ARequest->Content = _T("grant_type=");
                ARequest->Content << CHTTPServer::URLEncode(grant_type);

                ARequest->Content << _T("&assertion=");
                ARequest->Content << CHTTPServer::URLEncode(assertion);

                CHTTPRequest::Prepare(ARequest, _T("POST"), token_uri.c_str(), _T("application/x-www-form-urlencoded"));

                DebugRequest(ARequest);
            };

            auto OnException = [this](CTCPConnection *Sender, const Delphi::Exception::Exception &E) {

                auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                auto pClient = dynamic_cast<CHTTPClient *> (pConnection->Client());

                DebugReply(pConnection->Reply());

                m_FixedDate = Now() + (CDateTime) 60 / SecsPerDay;

                Log()->Error(APP_LOG_ERR, 0, "[%s:%d] %s", pClient->Host().c_str(), pClient->Port(), E.what());
            };

            CLocation token_uri(URI);

            auto pClient = GetClient(token_uri.hostname, token_uri.port);

            pClient->Data().Values("token_uri", token_uri.pathname);
            pClient->Data().Values("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
            pClient->Data().Values("assertion", Assertion);

            pClient->OnRequest(OnRequest);
            pClient->OnExecute(static_cast<COnSocketExecuteEvent &&>(OnDone));
            pClient->OnException(OnFailed == nullptr ? OnException : static_cast<COnSocketExceptionEvent &&>(OnFailed));

            pClient->Active(true);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::CreateAccessToken(const CProvider &Provider, const CString &Application,
                                                  CStringList &Tokens) {

            auto OnDone = [this](CTCPConnection *Sender) {

                auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                auto pReply = pConnection->Reply();

                DebugReply(pReply);

                if (pReply->Status == CHTTPReply::ok) {
                    const CJSON Json(pReply->Content);

                    m_Session = Json["session"].AsString();
                    m_Secret = Json["secret"].AsString();

                    m_FixedDate = 0;
                    m_ErrorCount = 0;

                    m_Status = psAuthorized;

                    m_CheckDate = Now() + (CDateTime) 55 / MinsPerDay; // 55 min
                }

                return true;
            };

            CString server_uri(m_Config["auth"]);

            const auto &token_uri = Provider.TokenURI(Application);
            const auto &service_token = CreateToken(Provider, Application);

            Tokens.Values("service_token", service_token);

            if (!token_uri.IsEmpty()) {
                FetchAccessToken(token_uri.front() == '/' ? server_uri + token_uri : token_uri, service_token, OnDone);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::CheckProviders() {
            for (int i = 0; i < m_Providers.Count(); i++) {
                auto& Provider = m_Providers[i].Value();
                if (Provider.KeyStatus() != ksUnknown) {
                    Provider.KeyStatusTime(Now());
                    Provider.KeyStatus(ksUnknown);
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::FetchProviders() {
            for (int i = 0; i < m_Providers.Count(); i++) {
                auto& Provider = m_Providers[i].Value();
                for (int j = 0; j < Provider.Applications().Count(); ++j) {
                    const auto &app = Provider.Applications().Members(j);
                    if (app["type"].AsString() == "service_account") {
                        if (Provider.KeyStatus() == ksUnknown) {
                            Provider.KeyStatusTime(Now());
                            CreateAccessToken(Provider, app.String(), m_Tokens[Provider.Name()]);
                            Provider.KeyStatus(ksSuccess);
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        CPQPollQuery *CReplicationServer::GetQuery(CPollConnection *AConnection) {
            auto pQuery = CServerProcess::GetQuery(AConnection);

            if (Assigned(pQuery)) {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                pQuery->OnPollExecuted([this](auto && APollQuery) { DoPostgresQueryExecuted(APollQuery); });
                pQuery->OnException([this](auto && APollQuery, auto && AException) { DoPostgresQueryException(APollQuery, AException); });
#else
                pQuery->OnPollExecuted(std::bind(&CReplicationServer::DoPostgresQueryExecuted, this, _1));
                pQuery->OnException(std::bind(&CReplicationServer::DoPostgresQueryException, this, _1, _2));
#endif
            }

            return pQuery;
        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationClient *CReplicationServer::GetReplicationClient() {
            auto pClient = m_ClientManager.Add(CLocation(m_Server + "/session/" + m_Session));

            pClient->Session() = m_Session;
            pClient->Secret() = m_Secret;

            pClient->ClientName() = GApplication->Title();
            pClient->AutoConnect(false);
            pClient->PollStack(PQClient().PollStack());

#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            pClient->OnVerbose([this](auto && Sender, auto && AConnection, auto && AFormat, auto && args) { DoVerbose(Sender, AConnection, AFormat, args); });
            pClient->OnWebSocketError([this](auto && AConnection) { DoWebSocketError(AConnection); });
            pClient->OnException([this](auto && AConnection, auto && AException) { DoException(AConnection, AException); });
            pClient->OnEventHandlerException([this](auto && AHandler, auto && AException) { DoServerEventHandlerException(AHandler, AException); });
            pClient->OnConnected([this](auto && Sender) { DoClientConnected(Sender); });
            pClient->OnDisconnected([this](auto && Sender) { DoClientDisconnected(Sender); });
            pClient->OnNoCommandHandler([this](auto && Sender, auto && AData, auto && AConnection) { DoNoCommandHandler(Sender, AData, AConnection); });
            pClient->OnMessage([this](auto && Sender, auto && Message) { DoClientMessage(Sender, Message); });
            pClient->OnError([this](auto && Sender, int Code, auto && Message) { DoClientError(Sender, Code, Message); });
            pClient->OnHeartbeat([this](auto && Sender) { DoClientHeartbeat(Sender); });
            pClient->OnTimeOut([this](auto && Sender) { DoClientTimeOut(Sender); });
            pClient->OnReplicationLog([this](auto && Sender, auto && Payload) { DoClientReplicationLog(Sender, Payload); });
            pClient->OnCheckReplicationLog([this](auto && Sender, auto && RelayId) { DoClientCheckReplicationLog(Sender, RelayId); });
#else
            pClient->OnVerbose(std::bind(&CReplicationServer::DoVerbose, this, _1, _2, _3, _4));
            pClient->OnWebSocketError(std::bind(&CReplicationServer::DoWebSocketError, this, _1));
            pClient->OnException(std::bind(&CReplicationServer::DoException, this, _1, _2));
            pClient->OnEventHandlerException(std::bind(&CReplicationServer::DoServerEventHandlerException, this, _1, _2));
            pClient->OnConnected(std::bind(&CReplicationServer::DoClientConnected, this, _1));
            pClient->OnDisconnected(std::bind(&CReplicationServer::DoClientDisconnected, this, _1));
            pClient->OnNoCommandHandler(std::bind(&CReplicationServer::DoNoCommandHandler, this, _1, _2, _3));
            pClient->OnMessage(std::bind(&CReplicationServer::DoClientMessage, this, _1, _2));
            pClient->OnError(std::bind(&CReplicationServer::DoClientError, this, _1, _2, _3));
            pClient->OnHeartbeat(std::bind(&CReplicationServer::DoClientHeartbeat, this, _1));
            pClient->OnTimeOut(std::bind(&CReplicationServer::DoClientTimeOut, this, _1));
            pClient->OnReplicationLog(std::bind(&CReplicationServer::DoClientReplicationLog, this, _1, _2));
            pClient->OnCheckReplicationLog(std::bind(&CReplicationServer::DoClientCheckReplicationLog, this, _1, _2));
#endif
            return pClient;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::CreateReplicationClient() {
            auto pClient = GetReplicationClient();
            try {
                InitActions(pClient);
                pClient->Source() = m_Source;
                pClient->Active(true);
            } catch (std::exception &e) {
                Log()->Error(APP_LOG_ERR, 0, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        int CReplicationServer::AddToQueue(CReplicationHandler *AHandler) {
            return m_Queue.AddToQueue(this, AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::InsertToQueue(int Index, CReplicationHandler *AHandler) {
            m_Queue.InsertToQueue(this, Index, AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::RemoveFromQueue(CReplicationHandler *AHandler) {
            m_Queue.RemoveFromQueue(this, AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::UnloadQueue() {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto queue = m_Queue[index];
                for (int i = 0; i < queue->Count(); ++i) {
                    auto pHandler = (CReplicationHandler *) queue->Item(i);
                    if (pHandler != nullptr) {
                        pHandler->Handler();
                        if (m_Progress >= m_MaxQueue)
                            break;
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DeleteHandler(CReplicationHandler *AHandler) {
            delete AHandler;
            DecProgress();
            UnloadQueue();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::InitActions(CReplicationClient *AClient) {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            AClient->Actions().AddObject(_T("/replication"), (CObject *) new CReplicationActionHandler(true , [this](auto && Sender, auto && Request, auto && Response) { OnReplication(Sender, Request, Response); }));
#else
            AClient->Actions().AddObject(_T("/replication")     , (CObject *) new CReplicationActionHandler(true , std::bind(&CReplicationServer::OnReplication, this, _1, _2, _3)));
#endif
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::InitServer() {
            if (m_ClientManager.Count() == 0) {
                CreateReplicationClient();
            }

            m_FixedDate = 0;
            m_Status = Process::psRunning;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::InitListen() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                try {
                    auto pResult = APollQuery->Results(0);

                    if (pResult->ExecStatus() != PGRES_COMMAND_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    APollQuery->Connection()->Listener(true);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                    APollQuery->Connection()->OnNotify([this](auto && APollQuery, auto && ANotify) { DoPostgresNotify(APollQuery, ANotify); });
#else
                    APollQuery->Connection()->OnNotify(std::bind(&CReplicationServer::DoPostgresNotify, this, _1, _2));
#endif
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoDataBaseError(E);
            };

            CStringList SQL;

            SQL.Add("LISTEN replication;");

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::CheckListen() {
            int index = 0;
            while (index < PQClient().PollManager().Count() && !PQClient().Connections(index)->Listener())
                index++;

            if (index == PQClient().PollManager().Count())
                InitListen();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::Apply() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                try {
                    auto pResult = APollQuery->Results(0);
                    if (pResult->ExecStatus() != PGRES_TUPLES_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    int count = 0;
                    if (!pResult->GetIsNull(0, 0)) {
                        count = StrToInt(pResult->GetValue(0, 0));
                    }

                    m_ApplyCount -= count;
                    if (m_ApplyCount < 0) {
                        m_ApplyCount = 0;
                    }

                    if (count > 0) {
                        m_ApplyDate = 0;
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DoDataBaseError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoDataBaseError(E);
            };

            CStringList SQL;

            try {
                api::replication_apply(SQL, m_Origin.Host());
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::CheckRelayLog(CReplicationClient *AClient) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                try {
                    auto pResult = APollQuery->Results(0);

                    if (pResult->ExecStatus() != PGRES_TUPLES_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    size_t relayId = 0;
                    if (!pResult->GetIsNull(0, 0)) {
                        relayId = StrToInt(pResult->GetValue(0, 0));
                    }

                    auto pConnection = dynamic_cast<CReplicationConnection *> (APollQuery->Binding());

                    if (pConnection != nullptr && !pConnection->ClosedGracefully()) {
                        pConnection->ReplicationClient()->Replication(relayId);
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoDataBaseError(E);
            };

            CStringList SQL;

            api::get_max_relay_id(SQL, m_Origin.Host());

            try {
                ExecSQL(SQL, AClient->Connection(), OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoTimer(CPollEventHandler *AHandler) {
            uint64_t exp;

            auto pTimer = dynamic_cast<CEPollTimer *> (AHandler->Binding());
            pTimer->Read(&exp, sizeof(uint64_t));

            try {
                DoHeartbeat();
                CModuleProcess::HeartbeatModules();
            } catch (Delphi::Exception::Exception &E) {
                DoServerEventHandlerException(AHandler, E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoError(const Delphi::Exception::Exception &E) {
            m_Session.Clear();
            m_Secret.Clear();

            m_FixedDate = 0;
            m_ApplyDate = 0;

            m_ErrorCount++;
            m_Status = psStopped;

            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoDataBaseError(const Delphi::Exception::Exception &E) {
            m_ErrorCount++;
            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::OnReplication(CObject *Sender, const CWSMessage &Request, CWSMessage &Response) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                CPQResult *pResult;

                try {
                    for (int i = 0; i < APollQuery->Count(); i++) {
                        pResult = APollQuery->Results(i);
                        if (pResult->ExecStatus() != PGRES_TUPLES_OK) {
                            throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                        }
                    }

                    m_ApplyCount++;
                    m_ApplyDate = Now() + (CDateTime) 1 / SecsPerDay;
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoDataBaseError(E);
            };

            auto pClient = dynamic_cast<CReplicationClient *> (Sender);

            chASSERT(pClient);

            CStringList SQL;

            if (Request.Payload.IsObject()) {
                const auto &caObject = Request.Payload.Object();
                api::add_to_relay_log(SQL, m_Origin.Host(), caObject["id"].AsLong(), caObject["datetime"].AsString(),
                                      caObject["action"].AsString(), caObject["schema"].AsString(),
                                      caObject["name"].AsString(), caObject["key"].ToString(),
                                      caObject["data"].ToString(), m_Mode == rmProxy);
            }

            try {
                ExecSQL(SQL, pClient->Connection(), OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoClientReplicationLog(CObject *Sender, const CJSON &Payload) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                CPQResult *pResult;
                size_t relayId = 0;

                try {
                    for (int i = 0; i < APollQuery->Count(); i++) {
                        pResult = APollQuery->Results(i);

                        if (pResult->ExecStatus() != PGRES_TUPLES_OK) {
                            throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                        }

                        if (!pResult->GetIsNull(0, 0)) {
                            relayId = StrToInt(pResult->GetValue(0, 0));
                            m_ApplyCount++;
                        }
                    }

                    Apply();

                    auto pConnection = dynamic_cast<CReplicationConnection *> (APollQuery->Binding());

                    if (pConnection != nullptr && !pConnection->ClosedGracefully()) {
                        pConnection->ReplicationClient()->Replication(relayId);
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoDataBaseError(E);
            };

            auto Add = [this](CStringList &SQL, const CJSONObject &Object) {
                api::add_to_relay_log(SQL, m_Origin.Host(), Object["id"].AsLong(), Object["datetime"].AsString(),
                                      Object["action"].AsString(), Object["schema"].AsString(),
                                      Object["name"].AsString(), Object["key"].ToString(),
                                      Object["data"].ToString(), m_Mode == rmProxy);
            };

            auto pClient = dynamic_cast<CReplicationClient *> (Sender);

            CStringList SQL;

            if (Payload.IsArray()) {
                const auto &caArray = Payload.Array();
                for (int i = 0; i < caArray.Count(); i++) {
                    Add(SQL, caArray[i].Object());
                }
            } else {
                const auto &caObject = Payload.Object();
                if (caObject.Count() != 0) {
                    Add(SQL, caObject);
                }
            }

            if (SQL.Count() == 0) {
                return;
            }

            try {
                ExecSQL(SQL, pClient->Connection(), OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoClientCheckReplicationLog(CObject *Sender, unsigned long RelayId) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                CPQResult *pResult;

                try {
                    auto pConnection = dynamic_cast<CReplicationConnection *> (APollQuery->Binding());

                    if (pConnection != nullptr && !pConnection->ClosedGracefully()) {
                        auto pClient = pConnection->ReplicationClient();

                        for (int i = 0; i < APollQuery->Count(); i++) {
                            pResult = APollQuery->Results(i);

                            if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                                throw Delphi::Exception::EDBError(pResult->GetErrorMessage());

                            m_NeedCheckReplicationLog = pResult->nTuples() > 0;

                            for (int row = 0; row < pResult->nTuples(); row++) {
                                pClient->SendData(pResult->GetValue(row, 0));
                            }
                        }
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoDataBaseError(E);
            };

            if (m_Status == psRunning && m_Mode == rmMaster) {
                auto pClient = dynamic_cast<CReplicationClient *> (Sender);

                chASSERT(pClient);

                CStringList SQL;

                api::replication_log(SQL, RelayId);

                try {
                    ExecSQL(SQL, pClient->Connection(), OnExecuted, OnException);
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoClientHeartbeat(CObject *Sender) {
            auto pClient = dynamic_cast<CReplicationClient *> (Sender);
            chASSERT(pClient);

            CheckRelayLog(pClient);

            if (m_NeedCheckReplicationLog) {
                pClient->SendGetMaxRelay();
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoClientTimeOut(CObject *Sender) {
            auto pClient = dynamic_cast<CReplicationClient *> (Sender);
            chASSERT(pClient);
            pClient->SwitchConnection(nullptr);
            pClient->Reload();
            m_FixedDate = 0;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoClientMessage(CObject *Sender, const CWSMessage &Message) {
            auto pClient = dynamic_cast<CReplicationClient *> (Sender);
            chASSERT(pClient);
            Log()->Message("[%s] [%s] [%s] [%s] %s", pClient->Session().c_str(),
                           Message.UniqueId.c_str(),
                           Message.Action.IsEmpty() ? "Unknown" : Message.Action.c_str(),
                           CWSMessage::MessageTypeIdToString(Message.MessageTypeId).c_str(),
                           Message.Payload.IsNull() ? "{}" : Message.Payload.ToString().c_str());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoClientError(CObject *Sender, int Code, const CString &Message) {
            Log()->Error(APP_LOG_ERR, 0, "[%d] %s", Code, Message.c_str());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoClientConnected(CObject *Sender) {
            auto pConnection = dynamic_cast<CReplicationConnection *>(Sender);
            if (pConnection != nullptr) {
                auto pBinding = pConnection->Socket()->Binding();
                if (pBinding != nullptr) {
                    Log()->Notice(_T("[%s:%d] [%s] Replication client connected."),
                                  pConnection->Socket()->Binding()->IP(),
                                  pConnection->Socket()->Binding()->Port(),
                                  pConnection->Session().c_str());
                } else {
                    Log()->Notice(_T("[%s] Replication client connected."),
                                  pConnection->Session().c_str());
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoClientDisconnected(CObject *Sender) {
            auto pConnection = dynamic_cast<CReplicationConnection *>(Sender);
            if (pConnection != nullptr) {
                auto pBinding = pConnection->Socket()->Binding();
                if (pBinding != nullptr) {
                    Log()->Notice(_T("[%s:%d] [%s] Replication client disconnected."),
                                  pConnection->Socket()->Binding()->IP(),
                                  pConnection->Socket()->Binding()->Port(),
                                  pConnection->Session().c_str());
                } else {
                    Log()->Notice(_T("[%s] Replication client disconnected."),
                                  pConnection->Session().c_str());
                }
            }

            if (m_Session.IsEmpty()) {
                m_Status = psAuthorization;
            } else {
                m_Status = psAuthorized;
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoReplication(CReplicationHandler *AHandler) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {

                auto pHandler = dynamic_cast<CReplicationHandler *> (APollQuery->Binding());

                if (pHandler == nullptr) {
                    return;
                }

                try {
                    auto pResult = APollQuery->Results(0);

                    if (pResult->ExecStatus() != PGRES_TUPLES_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    if (!pResult->GetIsNull(0, 0)) {
                        for (int i = 0; i < m_ClientManager.Count(); ++i) {
                            auto pClient = m_ClientManager.Items(i);
                            if (pClient->Active() && pClient->Connected() && !pClient->Connection()->ClosedGracefully()) {
                                pClient->SendData(pResult->GetValue(0, 0));
                            }
                        }
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DoDataBaseError(E);
                }

                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                auto pHandler = dynamic_cast<CReplicationHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            CStringList SQL;

            api::get_replication_log(SQL, AHandler->ReplicationId());

            try {
                ExecSQL(SQL, AHandler, OnExecuted, OnException);
                AHandler->Allow(false);
                IncProgress();
            } catch (Delphi::Exception::Exception &E) {
                DeleteHandler(AHandler);
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoPostgresNotify(CPQConnection *AConnection, PGnotify *ANotify) {
#ifdef _DEBUG
            const auto& Info = AConnection->ConnInfo();

            DebugMessage("[NOTIFY] [%d] [postgresql://%s@%s:%s/%s] [PID: %d] [%s] %s\n",
                         AConnection->Socket(), Info["user"].c_str(), Info["host"].c_str(), Info["port"].c_str(), Info["dbname"].c_str(),
                         ANotify->be_pid, ANotify->relname, ANotify->extra);
#endif
            if (m_Status == psRunning) {
                const CJSON Json(ANotify->extra);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                new CReplicationHandler(this, Json["id"].AsLong(), [this](auto &&Handler) { DoReplication(Handler); });
#else
                new CReplicationHandler(this, Json["id"].AsLong(), std::bind(&CReplicationServer::DoReplication, this, _1));
#endif
                UnloadQueue();
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoHeartbeat() {
            const auto now = Now();

            if ((now >= m_CheckDate)) {
                m_CheckDate = now + (CDateTime) 30 / SecsPerDay; // 30 sec
                m_Status = Process::psAuthorization;

                CheckProviders();
                FetchProviders();

                if (m_Mode == rmMaster) {
                    CheckListen();
                }
            }

            if (m_Status == Process::psAuthorized) {
                if ((now >= m_FixedDate)) {
                    m_FixedDate = now + (CDateTime) 30 / SecsPerDay; // 30 sec
                    m_Status = Process::psInProgress;

                    InitServer();
                }
            }

            if (m_Status == psRunning) {
                if ((now >= m_FixedDate)) {
                    m_FixedDate = now + (CDateTime) 30 / SecsPerDay; // 30 sec

                    for (int i = 0; i < m_ClientManager.Count(); ++i) {
                        auto pClient = m_ClientManager.Items(i);

                        if (!pClient->Active())
                            pClient->Active(true);

                        if (!pClient->Connected()) {
                            Log()->Notice(_T("[%s] Trying connect to %s."), pClient->Session().IsEmpty() ? "<null>" : pClient->Session().c_str(), pClient->URI().href().c_str());
                            pClient->ConnectStart();
                        }
                    }
                }

                if (m_ApplyCount >= 0 && now >= m_ApplyDate) {
                    m_ApplyDate = now + (CDateTime) 60 / MinsPerDay; // 60 min
                    Apply();
                }

                if (m_Mode == rmMaster) {
                    UnloadQueue();
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoWebSocketError(CTCPConnection *AConnection) {
            auto pConnection = dynamic_cast<CReplicationConnection *> (AConnection);
            auto pClient = dynamic_cast<CReplicationClient *> (pConnection->Client());
            auto pReply = pConnection->Reply();

            if (pReply->Status == CHTTPReply::moved_permanently || pReply->Status == CHTTPReply::moved_temporarily) {
                const auto &caLocation = pReply->Headers["Location"];
                if (!caLocation.IsEmpty()) {
                    pClient->SetURI(CLocation(caLocation));
                    Log()->Notice(_T("[%s] Redirect to %s."), pClient->Session().c_str(), pClient->URI().href().c_str());
                }
                m_FixedDate = 0;
            } else {
                auto pBinding = pConnection->Socket()->Binding();
                if (pBinding != nullptr) {
                    Log()->Warning(_T("[%s:%d] [%s] Replication client failed to establish a WS connection"),
                                   pConnection->Socket()->Binding()->IP(),
                                   pConnection->Socket()->Binding()->Port(),
                                   pConnection->Session().c_str());
                } else {
                    Log()->Warning(_T("[%s] Replication client failed to establish a WS connection."),
                                   pConnection->Session().c_str());
                }
                m_FixedDate = Now() + (CDateTime) 1 / MinsPerDay; // 1 min
            }

            pConnection->CloseConnection(true);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoException(CTCPConnection *AConnection, const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
            sig_reopen = 1;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoPostgresQueryExecuted(CPQPollQuery *APollQuery) {
            CPQResult *pResult;

            try {
                for (int i = 0; i < APollQuery->Count(); i++) {
                    pResult = APollQuery->Results(i);
                    if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                }
            } catch (std::exception &e) {
                Log()->Error(APP_LOG_ERR, 0, "%s", e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationServer::DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

    }
}

}
