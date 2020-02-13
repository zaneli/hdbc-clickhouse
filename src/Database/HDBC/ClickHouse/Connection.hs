{-# LANGUAGE FlexibleContexts #-}
module Database.HDBC.ClickHouse.Connection (connectClickHouse, Impl.Connection(), Impl.ping) where

import Database.HDBC
import Database.HDBC.Types
import Database.HDBC.DriverUtils
import Control.Exception
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString (sendAll, recv)

import qualified Data.ByteString as B
import qualified Database.HDBC.ClickHouse.ConnectionImpl as Impl
import qualified Database.HDBC.ClickHouse.Protocol.Hello as Hello
import qualified Database.HDBC.ClickHouse.Protocol.Ping as Ping

connectClickHouse :: String -> Int -> String -> String -> String -> IO Impl.Connection
connectClickHouse host port database username password = mkConn host port database username password

mkConn :: String -> Int -> String -> String -> String -> IO Impl.Connection
mkConn host port database username password = do
  conn <- fconnect host port database username password
  return $ Impl.Connection {
                     Impl.disconnect = fclose conn,
                     Impl.commit = fcommit,
                     Impl.rollback = frollback,
                     Impl.run = frun conn,
                     Impl.runRaw = frunRaw conn,
                     Impl.ping = fping conn
           }

fconnect :: String -> Int -> String -> String -> String -> IO Socket
fconnect host port database username password = withSocketsDo $ do
                addrInfo <- getAddrInfo Nothing (Just host) (Just $ show port)
                let serverAddr = head addrInfo
                sock <- socket (addrFamily serverAddr) Stream defaultProtocol
                connect sock (addrAddress serverAddr)

                sendAll sock $ Hello.request database username password
                r <- recvAll sock
                case (B.uncons r) of
                  Just (0, bs) -> print $ Hello.response bs
                  Just (b, bs) -> throwIO $ userError $ "Unexpected Response: " ++ (show b)
                  Nothing      -> throwIO $ userError "Unexpected Empty Response"

                return sock

fping :: Socket -> IO String
fping sock = withSocketsDo $ do
  sendAll sock $ Ping.request
  pong <- recvAll sock
  return $ Ping.response pong

fclose :: Socket -> IO ()
fclose conn = close conn

-- TODO: not implemented
fcommit :: IO ()
fcommit = return ()

-- TODO: not implemented
frollback :: IO ()
frollback = return ()

-- TODO: not implemented
frun :: Socket -> String -> [SqlValue] -> IO Integer
frun conn sql args = do
  return 1

-- TODO: not implemented
frunRaw :: Socket -> String -> IO ()
frunRaw conn sql = do
  return ()

recvAll :: Socket -> IO B.ByteString
recvAll conn =
  recvAll' conn B.empty
  where
    recvAll' conn bs = do
      r <- recv conn 1024
      let bs' = bs `B.append` r
      if B.last r == 4 then (return bs') else recvAll' conn bs'
