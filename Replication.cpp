/*++

Program name:

 Апостол CRM

Module Name:

  Replication.cpp

Notices:

  Replication process

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#include "Core.hpp"
#include "Replication.hpp"
//----------------------------------------------------------------------------------------------------------------------

#define SERVICE_APPLICATION_NAME "service"
#define CONFIG_SECTION_NAME "process/Replication"
//----------------------------------------------------------------------------------------------------------------------

#define PG_LISTEN_NAME "replication"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace BackEnd {

        namespace api {

            void replication_log(CStringList &SQL, unsigned long RelayId, const CString &Source, unsigned int Limit = 500) {
                SQL.Add(CString().Format("SELECT row_to_json(r) FROM api.replication_log(%d, %s, %d) AS r ORDER BY id DESC;",
                                         RelayId,
                                         PQQuoteLiteral(Source).c_str(),
                                         Limit));
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

        CReplicationHandler::CReplicationHandler(CReplicationProcess *AModule, unsigned long ReplicationId,
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

        //-- CReplicationProcess ---------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CReplicationProcess::CReplicationProcess(CCustomProcess *AParent, CApplication *AApplication):
                inherited(AParent, AApplication, ptCustom, "replication process") {

            m_RelayId = 0;

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

        void CReplicationProcess::BeforeRun() {
            sigset_t set;

            Application()->Header(Application()->Name() + ": replication process");

            Log()->Debug(APP_LOG_DEBUG_CORE, MSG_PROCESS_START, GetProcessName(), Application()->Header().c_str());

            InitSignals();

            Reload();

            SetUser(Config()->User(), Config()->Group());

            InitializePQClients(Application()->Title(), 1, Config()->PostgresPollMin());

            SigProcMask(SIG_UNBLOCK, SigAddSet(&set));

            SetTimerInterval(1000);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::AfterRun() {
            CApplicationProcess::AfterRun();
            PQClientsStop();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::Run() {
            auto &PQClient = PQClientStart(_T("helper"));

            while (!sig_exiting) {

                Log()->Debug(APP_LOG_DEBUG_EVENT, _T("replication process cycle"));

                try {
                    PQClient.Wait();
                } catch (Delphi::Exception::Exception &E) {
                    Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
                }

                if (sig_terminate || sig_quit) {
                    if (sig_quit) {
                        sig_quit = 0;
                        Log()->Debug(APP_LOG_DEBUG_EVENT, _T("gracefully shutting down"));
                        Application()->Header(_T("replication process is shutting down"));
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

            Log()->Debug(APP_LOG_DEBUG_EVENT, _T("stop replication process"));
        }
        //--------------------------------------------------------------------------------------------------------------

        bool CReplicationProcess::DoExecute(CTCPConnection *AConnection) {
            return CModuleProcess::DoExecute(AConnection);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::Reload() {
            CServerProcess::Reload();

            m_Providers.Clear();
            m_Tokens.Clear();
            m_QueueManager.Count();
            m_ClientManager.Clear();

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

            Config()->IniFile().ReadSectionValues(CONFIG_SECTION_NAME, &m_Config);

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
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::CreateAccessToken(CProvider &Provider, const CString &Application, CStringList &Tokens) {

            auto OnDone = [this, &Provider](CTCPConnection *Sender) {

                auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                auto &Reply = pConnection->Reply();

                DebugReply(Reply);

                if (Reply.Status == CHTTPReply::ok) {
                    const CJSON Json(Reply.Content);

                    m_Session = Json["session"].AsString();
                    m_Secret = Json["secret"].AsString();

                    m_FixedDate = 0;
                    m_ErrorCount = 0;

                    m_Status = psAuthorized;

                    m_CheckDate = Now() + (CDateTime) 55 / MinsPerDay; // 55 min

                    Provider.KeyStatus(ksSuccess);
                }

                return true;
            };

            auto OnHTTPClient = [this, &Provider](const CLocation &URI) {
                Provider.KeyStatus(ksFailed);
                return GetClient(URI.hostname, URI.port);
            };

            CString server_uri(m_Config["auth"]);

            const auto &token_uri = Provider.TokenURI(Application);
            const auto &service_token = CToken::CreateToken(Provider, Application);

            Tokens.Values("service_token", service_token);

            if (!token_uri.IsEmpty()) {
                CToken::FetchAccessToken(token_uri.front() == '/' ? server_uri + token_uri : token_uri, service_token, OnHTTPClient, OnDone);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::CheckProviders() {
            for (int i = 0; i < m_Providers.Count(); i++) {
                auto& Provider = m_Providers[i].Value();
                for (int j = 0; j < Provider.Applications().Count(); ++j) {
                    const auto &app = Provider.Applications().Members(j);
                    if (app["type"].AsString() == "service_account") {
                        if (Provider.KeyStatus() != ksFetching) {
                            Provider.KeyStatusTime(Now());
                            Provider.KeyStatus(ksFetching);
                            CreateAccessToken(Provider, app.String(), m_Tokens[Provider.Name()]);
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        CPQPollQuery *CReplicationProcess::GetQuery(CPollConnection *AConnection) {
            auto pQuery = CServerProcess::GetQuery(AConnection);

            if (Assigned(pQuery)) {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                pQuery->OnPollExecuted([this](auto && APollQuery) { DoPostgresQueryExecuted(APollQuery); });
                pQuery->OnException([this](auto && APollQuery, auto && AException) { DoPostgresQueryException(APollQuery, AException); });
#else
                pQuery->OnPollExecuted(std::bind(&CReplicationProcess::DoPostgresQueryExecuted, this, _1));
                pQuery->OnException(std::bind(&CReplicationProcess::DoPostgresQueryException, this, _1, _2));
#endif
            }

            return pQuery;
        }
        //--------------------------------------------------------------------------------------------------------------

        CReplicationClient *CReplicationProcess::GetReplicationClient() {
            auto pClient = m_ClientManager.Add(CLocation(m_Server + "/session/" + m_Session));

            pClient->Session() = m_Session;
            pClient->Secret() = m_Secret;

            pClient->ClientName() = GApplication->Title();
            pClient->AutoConnect(false);
            pClient->AllocateEventHandlers(GetPQClient());

#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            pClient->OnVerbose([this](auto && Sender, auto && AConnection, auto && AFormat, auto && args) { DoVerbose(Sender, AConnection, AFormat, args); });
            pClient->OnException([this](auto && AConnection, auto && AException) { DoException(AConnection, AException); });
            pClient->OnEventHandlerException([this](auto && AHandler, auto && AException) { DoServerEventHandlerException(AHandler, AException); });
            pClient->OnWebSocketError([this](auto && AConnection) { DoWebSocketError(AConnection); });
            pClient->OnConnected([this](auto && Sender) { DoClientConnected(Sender); });
            pClient->OnDisconnected([this](auto && Sender) { DoClientDisconnected(Sender); });
            pClient->OnNoCommandHandler([this](auto && Sender, auto && AData, auto && AConnection) { DoNoCommandHandler(Sender, AData, AConnection); });
            pClient->OnMessage([this](auto && Sender, auto && Message) { DoClientMessage(Sender, Message); });
            pClient->OnError([this](auto && Sender, int Code, auto && Message) { DoClientError(Sender, Code, Message); });
            pClient->OnHeartbeat([this](auto && Sender) { DoClientHeartbeat(Sender); });
            pClient->OnTimeOut([this](auto && Sender) { DoClientTimeOut(Sender); });
            pClient->OnReplicationLog([this](auto && Sender, auto && Payload) { DoClientReplicationLog(Sender, Payload); });
            pClient->OnReplicationCheckLog([this](auto && Sender, auto && Id) { DoClientReplicationCheckLog(Sender, Id); });
            pClient->OnReplicationCheckRelay([this](auto && Sender, auto && RelayId) { DoClientReplicationCheckRelay(Sender, RelayId); });
#else
            pClient->OnVerbose(std::bind(&CReplicationProcess::DoVerbose, this, _1, _2, _3, _4));
            pClient->OnException(std::bind(&CReplicationProcess::DoException, this, _1, _2));
            pClient->OnEventHandlerException(std::bind(&CReplicationProcess::DoServerEventHandlerException, this, _1, _2));
            pClient->OnWebSocketError(std::bind(&CReplicationProcess::DoWebSocketError, this, _1));
            pClient->OnConnected(std::bind(&CReplicationProcess::DoClientConnected, this, _1));
            pClient->OnDisconnected(std::bind(&CReplicationProcess::DoClientDisconnected, this, _1));
            pClient->OnNoCommandHandler(std::bind(&CReplicationProcess::DoNoCommandHandler, this, _1, _2, _3));
            pClient->OnMessage(std::bind(&CReplicationProcess::DoClientMessage, this, _1, _2));
            pClient->OnError(std::bind(&CReplicationProcess::DoClientError, this, _1, _2, _3));
            pClient->OnHeartbeat(std::bind(&CReplicationProcess::DoClientHeartbeat, this, _1));
            pClient->OnTimeOut(std::bind(&CReplicationProcess::DoClientTimeOut, this, _1));
            pClient->OnReplicationLog(std::bind(&CReplicationProcess::DoClientReplicationLog, this, _1, _2));
            pClient->OnReplicationCheckLog(std::bind(&CReplicationProcess::DoClientReplicationCheckLog, this, _1, _2));
            pClient->OnReplicationCheckRelay(std::bind(&CReplicationProcess::DoClientReplicationCheckRelay, this, _1, _2));
#endif
            return pClient;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::CreateReplicationClient() {
            auto pClient = GetReplicationClient();
            try {
                InitActions(pClient);
                pClient->Source() = m_Source;
                pClient->Proxy(m_Mode == rmMaster);
                pClient->Active(true);
            } catch (std::exception &e) {
                Log()->Error(APP_LOG_ERR, 0, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        int CReplicationProcess::AddToQueue(CReplicationHandler *AHandler) {
            return m_Queue.AddToQueue(this, AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::InsertToQueue(int Index, CReplicationHandler *AHandler) {
            m_Queue.InsertToQueue(this, Index, AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::RemoveFromQueue(CReplicationHandler *AHandler) {
            m_Queue.RemoveFromQueue(this, AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::UnloadQueue() {
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

        void CReplicationProcess::DeleteHandler(CReplicationHandler *AHandler) {
            delete AHandler;
            DecProgress();
            UnloadQueue();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::InitActions(CReplicationClient *AClient) {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            AClient->Actions().AddObject(_T("/replication"), (CObject *) new CReplicationActionHandler(true , [this](auto && Sender, auto && Request, auto && Response) { OnReplication(Sender, Request, Response); }));
#else
            AClient->Actions().AddObject(_T("/replication")     , (CObject *) new CReplicationActionHandler(true , std::bind(&CReplicationProcess::OnReplication, this, _1, _2, _3)));
#endif
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::InitServer() {
            if (m_ClientManager.Count() == 0) {
                CreateReplicationClient();
            }

            m_FixedDate = 0;
            m_Status = Process::psRunning;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::InitListen() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                try {
                    auto pResult = APollQuery->Results(0);

                    if (pResult->ExecStatus() != PGRES_COMMAND_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    APollQuery->Connection()->Listeners().Add(PG_LISTEN_NAME);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                    APollQuery->Connection()->OnNotify([this](auto && APollQuery, auto && ANotify) { DoPostgresNotify(APollQuery, ANotify); });
#else
                    APollQuery->Connection()->OnNotify(std::bind(&CReplicationProcess::DoPostgresNotify, this, _1, _2));
#endif
                } catch (Delphi::Exception::Exception &E) {
                    DoDataBaseError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CStringList SQL;

            SQL.Add("LISTEN " PG_LISTEN_NAME ";");

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::CheckListen() {
            if (!GetPQClient().CheckListen(PG_LISTEN_NAME))
                InitListen();
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::Apply() {

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
                } catch (Delphi::Exception::Exception &E) {
                    DoDataBaseError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CStringList SQL;

            api::replication_apply(SQL, m_Origin.Host());

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::ApplyRelay(CWebSocketClientConnection *AConnection, size_t RelayId) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                try {
                    auto pConnection = dynamic_cast<CWebSocketClientConnection *> (APollQuery->Binding());
                    auto pResult = APollQuery->Results(0);

                    if (pResult->ExecStatus() != PGRES_TUPLES_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    m_ApplyCount--;
                    if (m_ApplyCount < 0) {
                        m_ApplyCount = 0;
                    }

                    Replication(pConnection);
                } catch (Delphi::Exception::Exception &E) {
                    DoDataBaseError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CStringList SQL;

            api::replication_apply_relay(SQL, m_Origin.Host(), RelayId);

            try {
                ExecSQL(SQL, AConnection, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::CheckRelayLog(CWebSocketClientConnection *AConnection) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                try {
                    auto pConnection = dynamic_cast<CWebSocketClientConnection *> (APollQuery->Binding());
                    auto pResult = APollQuery->Results(0);

                    if (pResult->ExecStatus() != PGRES_TUPLES_OK) {
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                    }

                    size_t relayId = 0;
                    if (!pResult->GetIsNull(0, 0)) {
                        relayId = StrToInt(pResult->GetValue(0, 0));
                    }

                    if (m_RelayId < relayId) {
                        m_RelayId = relayId;
                        Replication(pConnection);
                    }
                } catch (Delphi::Exception::Exception &E) {
                    DoDataBaseError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            CStringList SQL;

            api::get_max_relay_id(SQL, m_Origin.Host());

            try {
                ExecSQL(SQL, AConnection, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::Replication(CWebSocketClientConnection *AConnection) const {
            if (AConnection != nullptr && AConnection->Connected()) {
                auto pClient = dynamic_cast<CReplicationClient *> (AConnection->Client());
                pClient->Replication(m_RelayId);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoTimer(CPollEventHandler *AHandler) {
            uint64_t exp;

            auto pTimer = dynamic_cast<CEPollTimer *> (AHandler->Binding());
            pTimer->Read(&exp, sizeof(uint64_t));

            try {
                Heartbeat(AHandler->TimeStamp());
                CModuleProcess::HeartbeatModules(AHandler->TimeStamp());
            } catch (Delphi::Exception::Exception &E) {
                DoServerEventHandlerException(AHandler, E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoError(const Delphi::Exception::Exception &E) {
            m_Session.Clear();
            m_Secret.Clear();

            m_FixedDate = 0;
            m_ApplyDate = 0;

            m_ErrorCount++;
            m_Status = psStopped;

            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoDataBaseError(const Delphi::Exception::Exception &E) {
            m_ErrorCount++;
            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::OnReplication(CObject *Sender, const CWSMessage &Request, CWSMessage &Response) {

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
                    m_ApplyDate = 0;
                } catch (Delphi::Exception::Exception &E) {
                    DoDataBaseError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            auto pClient = dynamic_cast<CReplicationClient *> (Sender);

            CStringList SQL;

            if (Request.Payload.IsObject()) {
                const auto &caObject = Request.Payload.Object();
                if (caObject["source"].AsString() != m_Source) {
                    api::add_to_relay_log(SQL, m_Origin.Host(), caObject["id"].AsLong(),
                                          caObject["datetime"].AsString(),
                                          caObject["action"].AsString(), caObject["schema"].AsString(),
                                          caObject["name"].AsString(), caObject["key"].ToString(),
                                          caObject["data"].ToString(), m_Mode >= rmProxy);
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

        void CReplicationProcess::DoClientReplicationLog(CObject *Sender, const CJSON &Payload) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                CPQResult *pResult;
                int count = 0;

                auto pConnection = dynamic_cast<CWebSocketClientConnection *> (APollQuery->Binding());

                try {
                    for (int i = 0; i < APollQuery->Count(); i++) {
                        pResult = APollQuery->Results(i);

                        if (pResult->ExecStatus() != PGRES_TUPLES_OK) {
                            throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                        }

                        if (!pResult->GetIsNull(0, 0)) {
                            m_RelayId = StrToInt(pResult->GetValue(0, 0));
                            count++;
                        }
                    }

                    m_ApplyCount += count;

                    if (count == 1) {
                        ApplyRelay(pConnection, m_RelayId);
                    } else {
                        Apply();
                        Replication(pConnection);
                    }
                } catch (Delphi::Exception::Exception &E) {
                    Apply();
                    Replication(pConnection);
                    DoDataBaseError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            auto Add = [this](CStringList &SQL, const CJSONObject &Object) {
                if (Object["source"].AsString() != m_Source) {
                    api::add_to_relay_log(SQL, m_Origin.Host(), Object["id"].AsLong(), Object["datetime"].AsString(),
                                          Object["action"].AsString(), Object["schema"].AsString(),
                                          Object["name"].AsString(), Object["key"].ToString(),
                                          Object["data"].ToString(), m_Mode >= rmProxy);
                }
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
                pClient->SendGetMaxLog();
                return;
            }

            try {
                ExecSQL(SQL, pClient->Connection(), OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoClientReplicationCheckLog(CObject *Sender, unsigned long Id) {
            auto pClient = dynamic_cast<CReplicationClient *> (Sender);
            chASSERT(pClient);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoClientReplicationCheckRelay(CObject *Sender, unsigned long RelayId) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                CPQResult *pResult;

                try {
                    auto pConnection = dynamic_cast<CWebSocketClientConnection *> (APollQuery->Binding());

                    if (pConnection != nullptr && pConnection->Connected()) {
                        auto pClient = dynamic_cast<CReplicationClient *> (pConnection->Client());

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
                    DoDataBaseError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            if (m_Status == psRunning && m_Mode == rmMaster) {
                auto pClient = dynamic_cast<CReplicationClient *> (Sender);

                chASSERT(pClient);

                CStringList SQL;

                api::replication_log(SQL, RelayId, m_Origin.Host());

                try {
                    ExecSQL(SQL, pClient->Connection(), OnExecuted, OnException);
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoClientHeartbeat(CObject *Sender) {
            auto pClient = dynamic_cast<CReplicationClient *> (Sender);
            chASSERT(pClient);

            if (m_NeedCheckReplicationLog) {
                pClient->SendGetMaxRelay();
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoClientTimeOut(CObject *Sender) {
            auto pClient = dynamic_cast<CReplicationClient *> (Sender);
            chASSERT(pClient);
            m_FixedDate = 0;
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoClientMessage(CObject *Sender, const CWSMessage &Message) {
            auto pClient = dynamic_cast<CReplicationClient *> (Sender);
            chASSERT(pClient);
            Log()->Message("[%s] [%s] [%s] [%s] %s", pClient->Session().c_str(),
                           Message.UniqueId.c_str(),
                           Message.Action.IsEmpty() ? "Unknown" : Message.Action.c_str(),
                           CWSMessage::MessageTypeIdToString(Message.MessageTypeId).c_str(),
                           Message.Payload.IsNull() ? "{}" : Message.Payload.ToString().c_str());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoClientError(CObject *Sender, int Code, const CString &Message) {
            Log()->Error(APP_LOG_ERR, 0, "[%d] %s", Code, Message.c_str());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoClientConnected(CObject *Sender) {
            auto pConnection = dynamic_cast<CWebSocketClientConnection *>(Sender);
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

        void CReplicationProcess::DoClientDisconnected(CObject *Sender) {
            auto pConnection = dynamic_cast<CWebSocketClientConnection *>(Sender);
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

        void CReplicationProcess::DoReplication(CReplicationHandler *AHandler) {

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
                            if (pClient->Active() && pClient->Authorized() && !pClient->Connection()->ClosedGracefully()) {
                                pClient->SendData(pResult->GetValue(0, 0));
                            } else {
                                pClient->Data().Add(pResult->GetValue(0, 0));
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
                DoError(E);
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

        void CReplicationProcess::DoPostgresNotify(CPQConnection *AConnection, PGnotify *ANotify) {
            DebugNotify(AConnection, ANotify);

            if (m_Status == psRunning) {
                const CJSON Json(ANotify->extra);
                if (Json["source"].AsString() != m_Origin.Host()) {
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
                    new CReplicationHandler(this, Json["id"].AsLong(), [this](auto &&Handler) { DoReplication(Handler); });
#else
                    new CReplicationHandler(this, Json["id"].AsLong(), std::bind(&CReplicationProcess::DoReplication, this, _1));
#endif
                    UnloadQueue();
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::Heartbeat(CDateTime Now) {
            if ((Now >= m_CheckDate)) {
                m_CheckDate = Now + (CDateTime) 5 / SecsPerDay; // 5 sec
                m_Status = Process::psAuthorization;

                CheckProviders();

                if (m_Mode == rmMaster) {
                    CheckListen();
                }
            }

            if (m_Status == Process::psAuthorized) {
                if ((Now >= m_FixedDate)) {
                    m_FixedDate = Now + (CDateTime) 30 / SecsPerDay; // 30 sec
                    m_Status = Process::psInProgress;

                    InitServer();
                    Apply();
                }
            }

            if (m_Status == psRunning) {
                if ((Now >= m_FixedDate)) {
                    m_FixedDate = Now + (CDateTime) 30 / SecsPerDay; // 30 sec

                    for (int i = 0; i < m_ClientManager.Count(); ++i) {
                        auto pClient = m_ClientManager.Items(i);

                        if (!pClient->Active())
                            pClient->Active(true);

                        if (!pClient->Connected()) {
                            Log()->Notice(_T("[%s] Trying connect to %s."), pClient->Session().IsEmpty() ? "<null>" : pClient->Session().c_str(), pClient->URI().href().c_str());
                            pClient->ConnectStart();
                        } else {
                            if (m_ApplyCount == 0) {
                                CheckRelayLog(pClient->Connection());
                            }
                        }
                    }
                }

                if (m_ApplyCount >= 0 && Now >= m_ApplyDate) {
                    if (m_ApplyDate == 0) {
                        m_ApplyDate = Now;
                    } else {
                        m_ApplyDate = Now + (CDateTime) 5 / MinsPerDay; // 5 min
                        Apply();
                    }
                }

                if (m_Mode == rmMaster) {
                    UnloadQueue();
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoWebSocketError(CTCPConnection *AConnection) {
            auto pConnection = dynamic_cast<CWebSocketClientConnection *> (AConnection);
            auto pClient = dynamic_cast<CReplicationClient *> (pConnection->Client());
            auto &Reply = pConnection->Reply();

            if (Reply.Status == CHTTPReply::moved_permanently || Reply.Status == CHTTPReply::moved_temporarily) {
                const auto &caLocation = Reply.Headers["Location"];
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

        void CReplicationProcess::DoException(CTCPConnection *AConnection, const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_ERR, 0, "Replication: %s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CReplicationProcess::DoPostgresQueryExecuted(CPQPollQuery *APollQuery) {
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

        void CReplicationProcess::DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            Log()->Error(APP_LOG_ERR, 0, "%s", E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

    }
}

}
