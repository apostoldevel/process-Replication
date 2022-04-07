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

        //-- CReplicationServer ----------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        enum CReplicationMode { rmSlave = 0, rmMaster };

        class CReplicationServer: public CApplicationProcess, public CModuleProcess {
            typedef CApplicationProcess inherited;

        private:

            CLocation m_Origin;

            CProcessStatus m_Status;

            CReplicationMode m_Mode;

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

            void DoError(const Delphi::Exception::Exception &E);
            void DoDataBaseError(const Delphi::Exception::Exception &E);

            void DoWebSocketError(CTCPConnection *AConnection);

            void DoConnected(CObject *Sender);
            void DoDisconnected(CObject *Sender);

            void DoReplicationClientHeartbeat(CObject *Sender);
            void DoReplicationClientTimeOut(CObject *Sender);

            void DoReplicationClientMessage(CObject *Sender, const CWSMessage &Message);
            void DoReplicationClientError(CObject *Sender, int Code, const CString &Message);

            void DoReplicationClientLog(CObject *Sender, const CJSON &Payload);

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

        };
        //--------------------------------------------------------------------------------------------------------------

    }
}

using namespace Apostol::Replication;
}
#endif //APOSTOL_REPLICATION_SERVER_HPP
