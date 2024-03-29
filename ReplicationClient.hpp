/*++

Program name:

 Апостол CRM

Module Name:

  ReplicationClient.hpp

Notices:

  Replication Client

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_REPLICATION_CLIENT_HPP
#define APOSTOL_REPLICATION_CLIENT_HPP
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Replication {

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationClient ----------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CObject *Sender, const CJSON &Payload)> COnReplicationClientLog;
        typedef std::function<void (CObject *Sender, unsigned long Id)> COnReplicationClientCheckLog;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationClient: public CCustomWebSocketClient {
        private:

            CString m_Source;
            CString m_Secret;

            int m_SendCount;

            unsigned long m_MaxLogId;
            unsigned long m_MaxRelayId;

            CDateTime m_ApplyDate;

            bool m_Proxy;
            bool m_Authorized;

            int m_HeartbeatInterval;

            CDateTime m_PingDateTime;
            CDateTime m_PongDateTime;
            CDateTime m_HeartbeatDateTime;
            CDateTime m_RegistrationDateTime;

            TList<CWSMessage> m_MessageList;

            COnReplicationClientLog m_OnReplicationLog;
            COnReplicationClientCheckLog m_OnReplicationCheckLog;
            COnReplicationClientCheckLog m_OnReplicationCheckRelay;

            void CheckCallError(const CWSMessage &Error, const CWSMessage &Message);

            void PushData();
            void PushMessageList();

        protected:

            void Heartbeat(CDateTime Now) override;

            void DoPing(CObject *Sender) override;
            void DoPong(CObject *Sender) override;

            void DoReplicationLog(const CJSON &Payload);
            void DoReplicationCheckLog(unsigned long Id);
            void DoReplicationCheckRelay(unsigned long RelayId);

        public:

            CReplicationClient();
            explicit CReplicationClient(const CLocation &URI);

            bool Authorized() const;

            void SendAuthorize();
            void SendSubscribe();
            void SendApply();
            void SendGetMaxLog();
            void SendGetMaxRelay();
            void SendData(const CString &Data);

            void Replication(size_t RelayId);

            void Reload();

            unsigned long MaxLogId() const { return m_MaxLogId; }
            unsigned long MaxRelayId() const { return m_MaxRelayId; }

            CString &Source() { return m_Source; }
            const CString &Source() const { return m_Source; }

            CString &Secret() { return m_Secret; }
            const CString &Secret() const { return m_Secret; }

            bool Proxy() const { return m_Proxy; }
            void Proxy(bool Value) { m_Proxy = Value; }

            const COnReplicationClientLog &OnReplicationLog() const { return m_OnReplicationLog; }
            void OnReplicationLog(COnReplicationClientLog && Value) { m_OnReplicationLog = Value; }

            const COnReplicationClientCheckLog &OnReplicationCheckLog() const { return m_OnReplicationCheckLog; }
            void OnReplicationCheckLog(COnReplicationClientCheckLog && Value) { m_OnReplicationCheckLog = Value; }

            const COnReplicationClientCheckLog &OnReplicationCheckRelay() const { return m_OnReplicationCheckRelay; }
            void OnReplicationCheckRelay(COnReplicationClientCheckLog && Value) { m_OnReplicationCheckRelay = Value; }

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationClientItem ------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CReplicationClientManager;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationClientItem: public CCollectionItem, public CReplicationClient {
        public:

            explicit CReplicationClientItem(CReplicationClientManager *AManager);

            explicit CReplicationClientItem(CReplicationClientManager *AManager, const CLocation &URI);

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationClientManager ---------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CReplicationClientManager: public CCollection {
            typedef CCollection inherited;

        protected:

            CReplicationClientItem *GetItem(int Index) const override;

        public:

            CReplicationClientManager(): CCollection(this) {

            };

            ~CReplicationClientManager() override = default;

            CReplicationClientItem *Add(const CLocation &URI);

            CReplicationClientItem *Items(int Index) const override { return GetItem(Index); };

            CReplicationClientItem *operator[] (int Index) const override { return Items(Index); };

        };

    }
}

using namespace Apostol::Replication;
}

#endif //APOSTOL_REPLICATION_CLIENT_HPP
