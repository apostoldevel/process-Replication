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

        class CCustomReplicationClient;
        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CCustomReplicationClient *AClient, const CWSMessage &Request, CWSMessage &Response)> COnReplicationClientActionHandlerEvent;
        //--------------------------------------------------------------------------------------------------------------
        
        class CReplicationClientActionHandler: CObject {
        private:

            bool m_Allow;

            COnReplicationClientActionHandlerEvent m_Handler;

        public:

            CReplicationClientActionHandler(bool Allow, COnReplicationClientActionHandlerEvent && Handler): CObject(), m_Allow(Allow), m_Handler(Handler) {

            };

            bool Allow() const { return m_Allow; };

            void Handler(CCustomReplicationClient *AClient, const CWSMessage &Request, CWSMessage &Response) {
                if (m_Allow && m_Handler)
                    m_Handler(AClient, Request, Response);
            }

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationMessageHandler --------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CReplicationMessageHandlerManager;
        class CReplicationMessageHandler;
        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CReplicationMessageHandler *Handler, CWebSocketConnection *Connection)> COnMessageHandlerEvent;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationMessageHandler: public CCollectionItem {
        private:

            CWSMessage m_Message;

            COnMessageHandlerEvent m_Handler;

        public:

            CReplicationMessageHandler(CReplicationMessageHandlerManager *AManager, const CWSMessage &Message, COnMessageHandlerEvent && Handler);

            const CWSMessage &Message() const { return m_Message; }

            void Handler(CWebSocketConnection *AConnection);

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationMessageHandlerManager -------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        
        class CReplicationMessageHandlerManager: public CCollection {
            typedef CCollection inherited;

        private:

            CReplicationMessageHandler *Get(int Index) const;
            void Set(int Index, CReplicationMessageHandler *Value);

        public:

            CReplicationMessageHandlerManager(): CCollection(this) {

            }

            CReplicationMessageHandler *Add(CWebSocketConnection *AConnection, const CWSMessage &Message, COnMessageHandlerEvent &&Handler, uint32_t Key = 0);

            CReplicationMessageHandler *First() { return Get(0); };
            CReplicationMessageHandler *Last() { return Get(Count() - 1); };

            CReplicationMessageHandler *FindMessageById(const CString &Value) const;

            CReplicationMessageHandler *Handlers(int Index) const { return Get(Index); }
            void Handlers(int Index, CReplicationMessageHandler *Value) { Set(Index, Value); }

            CReplicationMessageHandler *operator[] (int Index) const override { return Handlers(Index); };

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationConnection ------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CReplicationClient;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationConnection: public CHTTPClientConnection {
            typedef CHTTPClientConnection inherited;

        private:

            CString m_Session {};

            CLocation m_URI {};

        public:

            explicit CReplicationConnection(CPollSocketClient *AClient) : CHTTPClientConnection(AClient) {
                CloseConnection(false);
            }

            ~CReplicationConnection() override = default;

            CReplicationClient *ReplicationClient() { return (CReplicationClient *) CHTTPClientConnection::Client(); };

            CString &Session() { return m_Session; };
            const CString &Session() const { return m_Session; };

            CLocation &URI() { return m_URI; };
            const CLocation &URI() const { return m_URI; };

        }; // CReplicationConnection

        //--------------------------------------------------------------------------------------------------------------

        //-- CCustomReplicationClient ----------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        class CReplicationClient;
        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CObject *Sender, const CWSMessage &Message)> COnReplicationClientMessage;
        typedef std::function<void (CObject *Sender, int Code, const CString &Message)> COnReplicationClientError;
        //--------------------------------------------------------------------------------------------------------------

        class CCustomReplicationClient: public CHTTPClient, public CGlobalComponent  {
            friend CReplicationClient;

        private:

            CReplicationConnection *m_pConnection;

            CEPollTimer *m_pTimer;

            CLocation m_URI;

            CString m_Key;
            CString m_Session;
            CString m_Secret;

            CStringList m_Actions {true};

            uint32_t m_ErrorCount;

            int m_TimerInterval;
            int m_HeartbeatInterval;

            CDateTime m_PingDateTime;
            CDateTime m_HeartbeatDateTime;
            CDateTime m_RegistrationDateTime;

            bool m_DelayedClose;
            bool m_Authorized;
            bool m_UpdateConnected;

            COnReplicationClientMessage m_OnMessage;
            COnReplicationClientError m_OnError;

            COnSocketConnectionEvent m_OnWebSocketError;
            COnSocketConnectionEvent m_OnProtocolError;

            void Handshake(CReplicationConnection *AConnection);

            void AddToConnection(CWebSocketConnection *AConnection);
            void DeleteFromConnection(CWebSocketConnection *AConnection);

            void SetUpdateConnected(bool Value);

            void SetTimerInterval(int Value);
            void UpdateTimer();

        protected:

            CReplicationMessageHandlerManager m_Messages;

            void DoWebSocket(CHTTPClientConnection *AConnection);
            void DoHTTP(CHTTPClientConnection *AConnection);

            void DoDebugWait(CObject *Sender);
            void DoDebugRequest(CObject *Sender);
            void DoDebugReply(CObject *Sender);

            void DoConnectStart(CIOHandlerSocket *AIOHandler, CPollEventHandler *AHandler) override;
            void DoConnect(CPollEventHandler *AHandler) override;
            bool DoExecute(CTCPConnection *AConnection) override;

            void DoTimer(CPollEventHandler *AHandler);

            virtual void Heartbeat() abstract;

            virtual void DoMessage(const CWSMessage &Message);
            virtual void DoError(int Code, const CString &Message);

            virtual void DoWebSocketError(CTCPConnection *AConnection);

        public:

            CCustomReplicationClient();

            explicit CCustomReplicationClient(const CLocation &URI);

            ~CCustomReplicationClient() override;

            void Initialize() override;

            bool Connected();

            void Ping();

            void SwitchConnection(CReplicationConnection *AConnection);

            void SendMessage(const CWSMessage &Message, bool ASendNow = false);
            void SendMessage(const CWSMessage &Message, COnMessageHandlerEvent &&Handler);

            void SendNotSupported(const CString &UniqueId, const CString &ErrorDescription, const CJSON &Payload = {});
            void SendProtocolError(const CString &UniqueId, const CString &ErrorDescription, const CJSON &Payload = {});
            void SendInternalError(const CString &UniqueId, const CString &ErrorDescription, const CJSON &Payload = {});

            void IncErrorCount();
            void ClearErrorCount() { m_ErrorCount = 0; };

            uint32_t ErrorCount() const { return m_ErrorCount; }

            static CWSMessage RequestToMessage(CWebSocketConnection *AWSConnection);

            CReplicationConnection *Connection() { return m_pConnection; };

            CReplicationMessageHandlerManager &Messages() { return m_Messages; };
            const CReplicationMessageHandlerManager &Messages() const { return m_Messages; };

            void UpdateConnected(bool Value) { SetUpdateConnected(Value); };
            bool UpdateConnected() const { return m_UpdateConnected; };

            bool DelayedClose() const { return m_DelayedClose; }
            void DelayedClose(bool Value) { m_DelayedClose = Value; }

            CString &Key() { return m_Key; }
            const CString &Key() const { return m_Key; }

            CLocation &URI() { return m_URI; }
            const CLocation &URI() const { return m_URI; }

            CString &Session() { return m_Session; }
            const CString &Session() const { return m_Session; }

            CString &Secret() { return m_Secret; }
            const CString &Secret() const { return m_Secret; }

            CStringList &Actions() { return m_Actions; }
            const CStringList &Actions() const { return m_Actions; }

            int TimerInterval() const { return m_TimerInterval; }
            void TimerInterval(int Value) { SetTimerInterval(Value); }

            static CCustomReplicationClient *FindOfConnection(CWebSocketConnection *AConnection);

            const COnReplicationClientMessage &OnMessage() const { return m_OnMessage; }
            void OnMessage(COnReplicationClientMessage && Value) { m_OnMessage = Value; }

            const COnReplicationClientError &OnError() const { return m_OnError; }
            void OnError(COnReplicationClientError && Value) { m_OnError = Value; }

            const COnSocketConnectionEvent &OnProtocolError() { return m_OnProtocolError; }
            void OnProtocolError(COnSocketConnectionEvent && Value) { m_OnProtocolError = Value; }

            const COnSocketConnectionEvent &OnWebSocketError() { return m_OnWebSocketError; }
            void OnWebSocketError(COnSocketConnectionEvent && Value) { m_OnWebSocketError = Value; }

        };

        //--------------------------------------------------------------------------------------------------------------

        //-- CReplicationClient ----------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        typedef std::function<void (CObject *Sender, const CJSON &Payload)> COnReplicationClientLog;
        //--------------------------------------------------------------------------------------------------------------

        class CReplicationClient: public CCustomReplicationClient {
        private:

            CString m_Source;

            bool m_SendApply;

            CNotifyEvent m_OnHeartbeat;

            COnReplicationClientLog m_OnReplicationLog;

            void Heartbeat() override;

        protected:

            void DoHeartbeat();

            void DoReplicationLog(const CJSON &Payload);

        public:

            CReplicationClient();
            explicit CReplicationClient(const CLocation &URI);

            void SendAuthorize();
            void SendSubscribe();
            void SendApply();
            void SendData(const CString &Data);

            void Replication(size_t RelayId);

            CString &Source() { return m_Source; }
            const CString &Source() const { return m_Source; }

            const CNotifyEvent &OnHeartbeat() const { return m_OnHeartbeat; }
            void OnHeartbeat(CNotifyEvent && Value) { m_OnHeartbeat = Value; }

            const COnReplicationClientLog &OnReplicationLog() const { return m_OnReplicationLog; }
            void OnReplicationLog(COnReplicationClientLog && Value) { m_OnReplicationLog = Value; }

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
