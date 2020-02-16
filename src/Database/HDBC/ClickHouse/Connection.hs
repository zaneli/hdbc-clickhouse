module Database.HDBC.ClickHouse.Connection (connectClickHouse, Impl.Connection(), Impl.ping) where

import Database.HDBC
import Database.HDBC.Types
import Database.HDBC.DriverUtils
import Control.Exception
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString (sendAll)
import Database.HDBC.ClickHouse.Protocol

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.ConnectionImpl as Impl
import qualified Database.HDBC.ClickHouse.Protocol.Hello as Hello
import qualified Database.HDBC.ClickHouse.Protocol.Ping as Ping
import qualified Database.HDBC.ClickHouse.Protocol.Query as Query

connectClickHouse :: Config -> IO Impl.Connection
connectClickHouse config =
  mkConn config

mkConn :: Config -> IO Impl.Connection
mkConn config = do
  (sock, serverInfo) <- fconnect config
  return $ Impl.Connection {
    Impl.disconnect = fclose sock,
    Impl.commit = fcommit,
    Impl.rollback = frollback,
    Impl.run = frun sock,
    Impl.runRaw = frunRaw sock serverInfo config,
    Impl.ping = fping sock
  }

fconnect :: Config -> IO (Socket, ServerInfo)
fconnect config = withSocketsDo $ do
  addrInfo <- getAddrInfo Nothing (Just $ host config ) (Just $ show $ port config)
  let serverAddr = head addrInfo
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  connect sock (addrAddress serverAddr)

  serverInfo <- Hello.send sock config

  return (sock, serverInfo)

fping :: Socket -> IO String
fping sock = withSocketsDo $ do
  Ping.send sock

fclose :: Socket -> IO ()
fclose sock = close sock

-- TODO: not implemented
fcommit :: IO ()
fcommit = return ()

-- TODO: not implemented
frollback :: IO ()
frollback = return ()

-- TODO: not implemented
frun :: Socket -> String -> [SqlValue] -> IO Integer
frun sock sql args = do
  return 1

frunRaw :: Socket -> ServerInfo -> Config -> String -> IO ()
frunRaw sock serverInfo config sql = do
  Query.send sock sql serverInfo config
  return ()
