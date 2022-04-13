/*++

Program name:

 Апостол CRM

Module Name:

  ReplicationServer.hpp

Notices:

  Replication Server

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_REPLICATION_SERVER_HPP
#define APOSTOL_REPLICATION_SERVER_HPP
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Replication {

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationActionHandler ---------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CCustomReplicationClient *Sender, const CWSMessage &Request, CWSMessage &Response)> COnReplicationActionHandlerEvent;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationActionHandler: CObject {
        private:

            bool m_Allow;

            COnReplicationActionHandlerEvent m_Handler;

        public:

            CReplicationActionHandler(bool Allow, COnReplicationActionHandlerEvent && Handler): CObject(), m_Allow(Allow), m_Handler(Handler) {

            };

            bool Allow() const { return m_Allow; };

            void Handler(CCustomReplicationClient *Sender, const CWSMessage &Request, CWSMessage &Response) {
                if (m_Allow && m_Handler)
                    m_Handler(Sender, Request, Response);
            }

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationHandler ---------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CReplicationServer;
        class CReplicationHandler;
        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CReplicationHandler *Handler)> COnReplicationHandlerEvent;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationHandler: public CPollConnection {
        private:

            CReplicationServer *m_pModule;

            unsigned long m_ReplicationId;

            bool m_Allow;

            COnReplicationHandlerEvent m_Handler;

            int AddToQueue();
            void RemoveFromQueue();

        protected:

            void SetAllow(bool Value) { m_Allow = Value; }

        public:

            CReplicationHandler(CReplicationServer *AModule, unsigned long ReplicationId, COnReplicationHandlerEvent && Handler);

            ~CReplicationHandler() override;

            unsigned long ReplicationId() const { return m_ReplicationId; }

            bool Allow() const { return m_Allow; };
            void Allow(bool Value) { SetAllow(Value); };

            bool Handler();

            void Close() override;
        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationServer ----------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        enum CReplicationMode { rmSlave = 0, rmMaster };

        class CReplicationServer: public CApplicationProcess, public CModuleProcess {
            typedef CApplicationProcess inherited;

        private:

            CLocation m_Origin;

            CProcessStatus m_Status;

            CReplicationMode m_Mode;

            int m_ApplyCount;

            bool m_NeedCheckReplicationLog;

            CString m_Session;
            CString m_Secret;

            CString m_Source;
            CString m_Server;

            uint32_t m_ErrorCount;

            CStringList m_Config;

            CDateTime m_CheckDate;
            CDateTime m_FixedDate;
            CDateTime m_ApplyDate;

            CProviders m_Providers;
            CStringListPairs m_Tokens;

            CReplicationClientManager m_ClientManager;

            CQueue m_Queue;
            CQueueManager m_QueueManager;

            size_t m_Progress;
            size_t m_MaxQueue;

            void BeforeRun() override;
            void AfterRun() override;

            void CheckListen();
            void InitListen();

            void Apply();
            void CheckRelayLog(CReplicationClient *AClient);

            void InitActions(CReplicationClient *AClient);
            void InitServer();

            CReplicationClient *GetReplicationClient();
            void CreateReplicationClient();

            void UnloadQueue();
            void DeleteHandler(CReplicationHandler *AHandler);

            void CheckProviders();
            void FetchProviders();

            void FetchAccessToken(const CString &URI, const CString &Assertion,
                COnSocketExecuteEvent && OnDone, COnSocketExceptionEvent && OnFailed = nullptr);
            void CreateAccessToken(const CProvider &Provider, const CString &Application, CStringList &Tokens);

            static CString CreateToken(const CProvider &Provider, const CString &Application);

            void OnReplication(CObject *Sender, const CWSMessage &Request, CWSMessage &Response);

        protected:

            void DoTimer(CPollEventHandler *AHandler) override;

            void DoHeartbeat();

            void DoReplication(CReplicationHandler *AHandler);

            void DoError(const Delphi::Exception::Exception &E);
            void DoDataBaseError(const Delphi::Exception::Exception &E);

            void DoWebSocketError(CTCPConnection *AConnection);

            void DoClientConnected(CObject *Sender) override;
            void DoClientDisconnected(CObject *Sender) override;

            void DoClientHeartbeat(CObject *Sender);
            void DoClientTimeOut(CObject *Sender);

            void DoClientMessage(CObject *Sender, const CWSMessage &Message);
            void DoClientError(CObject *Sender, int Code, const CString &Message);

            void DoClientReplicationLog(CObject *Sender, const CJSON &Payload);
            void DoClientCheckReplicationLog(CObject *Sender, unsigned long RelayId);

            void DoException(CTCPConnection *AConnection, const Delphi::Exception::Exception &E);
            bool DoExecute(CTCPConnection *AConnection) override;

            void DoPostgresNotify(CPQConnection *AConnection, PGnotify *ANotify);

            void DoPostgresQueryExecuted(CPQPollQuery *APollQuery);
            void DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E);

        public:

            explicit CReplicationServer(CCustomProcess* AParent, CApplication *AApplication);

            ~CReplicationServer() override = default;

            static class CReplicationServer *CreateProcess(CCustomProcess *AParent, CApplication *AApplication) {
                return new CReplicationServer(AParent, AApplication);
            }

            void Run() override;
            void Reload() override;

            CPQPollQuery *GetQuery(CPollConnection *AConnection) override;

            void IncProgress() { m_Progress++; }
            void DecProgress() { m_Progress--; }

            int AddToQueue(CReplicationHandler *AHandler);
            void InsertToQueue(int Index, CReplicationHandler *AHandler);
            void RemoveFromQueue(CReplicationHandler *AHandler);

            CQueue &Queue() { return m_Queue; }
            const CQueue &Queue() const { return m_Queue; }

            CPollManager *ptrQueueManager() { return &m_QueueManager; }

            CPollManager &QueueManager() { return m_QueueManager; }
            const CPollManager &QueueManager() const { return m_QueueManager; }

        };
        //--------------------------------------------------------------------------------------------------------------

    }
}

using namespace Apostol::Replication;
}
#endif //APOSTOL_REPLICATION_SERVER_HPP
