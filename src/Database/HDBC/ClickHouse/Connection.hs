{-# LANGUAGE FlexibleContexts #-}
module Database.HDBC.ClickHouse.Connection (connectClickHouse, Impl.Connection(), Impl.ping) where

import Database.HDBC
import Database.HDBC.Types
import Database.HDBC.DriverUtils
import Control.Exception
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString (sendAll)

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.ConnectionImpl as Impl
import qualified Database.HDBC.ClickHouse.Protocol.Hello as Hello
import qualified Database.HDBC.ClickHouse.Protocol.Ping as Ping
import qualified Database.HDBC.ClickHouse.Protocol.Query as Query

connectClickHouse :: String -> Int -> String -> String -> String -> Bool -> IO Impl.Connection
connectClickHouse host port database username password debug =
  mkConn host port database username password debug

mkConn :: String -> Int -> String -> String -> String -> Bool -> IO Impl.Connection
mkConn host port database username password debug = do
  (sock, revision) <- fconnect host port database username password debug
  return $ Impl.Connection {
    Impl.disconnect = fclose sock,
    Impl.commit = fcommit,
    Impl.rollback = frollback,
    Impl.run = frun sock,
    Impl.runRaw = frunRaw sock revision,
    Impl.ping = fping sock
  }

fconnect :: String -> Int -> String -> String -> String -> Bool -> IO (Socket, Int)
fconnect host port database username password debug = withSocketsDo $ do
  addrInfo <- getAddrInfo Nothing (Just host) (Just $ show port)
  let serverAddr = head addrInfo
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  connect sock (addrAddress serverAddr)

  (_, _, _, revision, _) <- Hello.send sock database username password debug

  return (sock, revision)

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

-- TODO: not implemented
frunRaw :: Socket -> Int -> String -> IO ()
frunRaw sock revision sql = do
  return ()
