/*++

Program name:

 Апостол CRM

Module Name:

  Replication.hpp

Notices:

  Replication process

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_REPLICATION_PROCESS_HPP
#define APOSTOL_REPLICATION_PROCESS_HPP
//----------------------------------------------------------------------------------------------------------------------

#include "Replication/ReplicationClient.hpp"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Replication {

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationActionHandler ---------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CCustomWebSocketClient *Sender, const CWSMessage &Request, CWSMessage &Response)> COnReplicationActionHandlerEvent;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationActionHandler: CObject {
        private:

            bool m_Allow;

            COnReplicationActionHandlerEvent m_Handler;

        public:

            CReplicationActionHandler(bool Allow, COnReplicationActionHandlerEvent && Handler): CObject(), m_Allow(Allow), m_Handler(Handler) {

            };

            bool Allow() const { return m_Allow; };

            void Handler(CCustomWebSocketClient *Sender, const CWSMessage &Request, CWSMessage &Response) {
                if (m_Allow && m_Handler)
                    m_Handler(Sender, Request, Response);
            }

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationHandler ---------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CReplicationProcess;
        class CReplicationHandler;
        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CReplicationHandler *Handler)> COnReplicationHandlerEvent;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationHandler: public CPollConnection {
        private:

            CReplicationProcess *m_pModule;

            unsigned long m_ReplicationId;

            bool m_Allow;

            COnReplicationHandlerEvent m_Handler;

            int AddToQueue();
            void RemoveFromQueue();

        protected:

            void SetAllow(bool Value) { m_Allow = Value; }

        public:

            CReplicationHandler(CReplicationProcess *AModule, unsigned long ReplicationId, COnReplicationHandlerEvent && Handler);

            ~CReplicationHandler() override;

            unsigned long ReplicationId() const { return m_ReplicationId; }

            bool Allow() const { return m_Allow; };
            void Allow(bool Value) { SetAllow(Value); };

            bool Handler();

            void Close() override;
        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationProcess ---------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        typedef CPollManager CQueueManager;
        //--------------------------------------------------------------------------------------------------------------

        enum CReplicationMode { rmSlave = 0, rmProxy, rmMaster };
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationProcess: public CApplicationProcess, public CModuleProcess {
            typedef CApplicationProcess inherited;

        private:

            CLocation m_Origin;

            CProcessStatus m_Status;

            CReplicationMode m_Mode;

            size_t m_RelayId;

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
            void ApplyRelay(CWebSocketClientConnection *AConnection, size_t RelayId);
            void CheckRelayLog(CWebSocketClientConnection *AConnection);
            void Replication(CWebSocketClientConnection *AConnection) const;

            void InitActions(CReplicationClient *AClient);
            void InitServer();

            CReplicationClient *GetReplicationClient();
            void CreateReplicationClient();

            void UnloadQueue();
            void DeleteHandler(CReplicationHandler *AHandler);

            void CheckProviders();

            void Heartbeat(CDateTime Now);

            void CreateAccessToken(CProvider &Provider, const CString &Application, CStringList &Tokens);

            void OnReplication(CObject *Sender, const CWSMessage &Request, CWSMessage &Response);

        protected:

            void DoTimer(CPollEventHandler *AHandler) override;

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
            void DoClientReplicationCheckLog(CObject *Sender, unsigned long Id);
            void DoClientReplicationCheckRelay(CObject *Sender, unsigned long RelayId);

            void DoException(CTCPConnection *AConnection, const Delphi::Exception::Exception &E);
            bool DoExecute(CTCPConnection *AConnection) override;

            void DoPostgresNotify(CPQConnection *AConnection, PGnotify *ANotify);

            void DoPostgresQueryExecuted(CPQPollQuery *APollQuery);
            void DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E);

        public:

            explicit CReplicationProcess(CCustomProcess* AParent, CApplication *AApplication);

            ~CReplicationProcess() override = default;

            static class CReplicationProcess *CreateProcess(CCustomProcess *AParent, CApplication *AApplication) {
                return new CReplicationProcess(AParent, AApplication);
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
#endif //APOSTOL_REPLICATION_PROCESS_HPP
