module Database.HDBC.ClickHouse.Connection (connectClickHouse, Impl.Connection(), Impl.ping) where

import Control.Exception
import Data.List (findIndex)
import Database.HDBC
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Database.HDBC.ClickHouse.Data
import Database.HDBC.ClickHouse.Data.ColDesc
import Database.HDBC.ClickHouse.Data.Creation
import Database.HDBC.ClickHouse.Exception
import Database.HDBC.ClickHouse.Protocol
import Database.HDBC.ColTypes
import Network.HostName

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.ConnectionImpl as Impl
import qualified Database.HDBC.ClickHouse.Statement as Stmt
import qualified Database.HDBC.ClickHouse.Protocol.Hello as Hello
import qualified Database.HDBC.ClickHouse.Protocol.Ping as Ping
import qualified Database.HDBC.ClickHouse.Protocol.Query as Query

connectClickHouse :: Config -> IO Impl.Connection
connectClickHouse config =
  mkConn config

mkConn :: Config -> IO Impl.Connection
mkConn config = do
  clientInfo <- fmap createClientInfo getHostName
  (sock, serverInfo) <- fconnect clientInfo config
  let clientVer = clientVersion clientInfo
  return $ Impl.Connection {
    Impl.disconnect = fclose sock,
    Impl.commit = fcommit,
    Impl.rollback = frollback,
    Impl.run = frun sock,
    Impl.runRaw = frunRaw sock clientInfo serverInfo config,
    Impl.prepare = Stmt.fprepare sock clientInfo serverInfo config,
    Impl.clone = connectClickHouse config,
    Impl.hdbcDriverName = "clickhouse",
    Impl.hdbcClientVer = clientVer,
    Impl.proxiedClientName = "clickhouse",
    Impl.proxiedClientVer = clientVer,
    Impl.dbTransactionSupport = True,
    Impl.dbServerVer = serverVersion serverInfo,
    Impl.getTables = fgetTables sock clientInfo serverInfo config,
    Impl.describeTable = fdescribeTable sock clientInfo serverInfo config,
    Impl.ping = fping sock
  }

fconnect :: ClientInfo -> Config -> IO (Socket, ServerInfo)
fconnect clientInfo config = withSocketsDo $ do
  addrInfo <- getAddrInfo Nothing (Just $ host config) (Just $ show $ port config)
  let serverAddr = head addrInfo
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  connect sock (addrAddress serverAddr)
  serverInfo <- Hello.send sock clientInfo config
  return (sock, serverInfo)

fping :: Socket -> IO String
fping sock = withSocketsDo $ do
  Ping.send sock

fclose :: Socket -> IO ()
fclose sock = close sock

fcommit :: IO ()
fcommit =
  throwIO $ userError "fcommit not implemented"

frollback :: IO ()
frollback =
  throwIO $ userError "frollback not implemented"

frun :: Socket -> String -> [SqlValue] -> IO Integer
frun sock sql args =
  throwIO $ userError "frun not implemented"

frunRaw :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> IO ()
frunRaw sock clientInfo serverInfo config sql = do
  Query.send sock sql clientInfo serverInfo config
  return ()

fgetTables :: Socket -> ClientInfo -> ServerInfo -> Config -> IO [String]
fgetTables sock clientInfo serverInfo config = do
  (columns, values) <- Query.send sock "show tables" clientInfo serverInfo config
  return $ concatMap (map fromSql) values

fdescribeTable :: Socket -> ClientInfo -> ServerInfo -> Config -> String -> IO [(String, SqlColDesc)]
fdescribeTable sock clientInfo serverInfo config table = do
  (columns, values) <- Query.send sock ("desc " ++ table) clientInfo serverInfo config
  case (findIndex (\c -> columnName c == "name") columns, findIndex (\c -> columnName c == "type") columns) of
    (Just nameIdx, Just typeIdx) ->
      return $ map (\v -> f (fromSql $ v !! nameIdx) (fromSql $ v !! typeIdx)) values
    _ -> throwIO $ ClientException "table metadata not found"
  where
    f name typ =
      let column = createColumn name typ
          desc   = getSqlColDesc column
      in (name, desc)
