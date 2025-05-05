import java.net.*;
import java.io.*;
import java.util.Timer;
import java.util.Hashtable;

class StudentSocketImpl extends BaseSocketImpl {

  // SocketImpl data members:
  //   protected InetAddress address;
  //   protected int port;
  //   protected int localport;

  private Demultiplexer D;
  private Timer tcpTimer;

  public enum State {
    CLOSED, 
    LISTEN,
    SYN_SENT,
    SYN_RCVD,
    ESTABLISHED,
    FIN_WAIT_1,
    FIN_WAIT_2,
    CLOSE_WAIT,
    CLOSING,
    LAST_ACK,
    TIME_WAIT
  }

  public State currentState = State.CLOSED;
  private static int initSeqNum = 100;
  private static int initAckNum = 150;
  private int seqNum = initSeqNum; 
  private int ackNum = initAckNum;
  private int windowSize = 1;
  private Hashtable<State, TCPTimerTask> timerTable = new Hashtable<>();
  private Hashtable<State, TCPPacket> packetTable = new Hashtable<>();


  StudentSocketImpl(Demultiplexer D) {  // default constructor
    this.D = D;
  }

  /**
   * Connects this socket to the specified port number on the specified host.
   *
   * @param      address   the IP address of the remote host.
   * @param      port      the port number.
   * @exception  IOException  if an I/O error occurs when attempting a
   *               connection.
   */
  public synchronized void connect(InetAddress address, int port) throws IOException{
    localport = D.getNextAvailablePort();
    D.registerConnection(address, localport, port, this);
    this.address = address;

    changeStates(State.SYN_SENT);

    TCPPacket synPack = new TCPPacket(localport, port, seqNum, 0, false, true, false, windowSize, null);
    TCPWrapper.send(synPack, address);

    // wait until state has advanced to ESTABLISHED before returning 
    while (currentState != State.ESTABLISHED) {
      try {
        this.wait();
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * Called by Demultiplexer when a packet comes in for this connection
   * @param p The packet that arrived
   */
  public synchronized void receivePacket(TCPPacket p){
    // System.out.println(p.getDebugOutput());
    // System.out.flush();

    if (p.ackFlag && timerTable.containsKey(currentState)) {
      timerTable.get(currentState).cancel();
      timerTable.remove(currentState);
      packetTable.remove(currentState);
    }

    else if (p.synFlag && (currentState == State.LISTEN)) {
      System.out.println("SYN YESSIR");

      changeStates(State.SYN_RCVD);
      setPacketInfo(p);

      try {
        D.unregisterListeningSocket(localport, this);
        D.registerConnection(address, localport, port, this);
      }
      catch (IOException e) {
        e.printStackTrace();
      }

      
      TCPPacket synAckPack = new TCPPacket(localport, port, seqNum, ackNum, true, true, false, windowSize, null);
      sendPacket(synAckPack);
    }

    else if (p.synFlag && p.ackFlag && (currentState == State.SYN_SENT)) {
      System.out.println("SYNACK YESSIR");

      changeStates(State.ESTABLISHED);
      ackNum = p.ackNum;
      setPacketInfo(p);

      TCPPacket ackPack = new TCPPacket(localport, port, seqNum, ackNum, true, false, false, windowSize, null);
      sendPacket(ackPack);

    }

    else if (p.ackFlag && (currentState == State.SYN_RCVD)) {
      System.out.println("ACK YESSIR");
 
      changeStates(State.ESTABLISHED);
    }

    else if (p.finFlag && (currentState == State.ESTABLISHED)) {
      // go to closewait
      changeStates(State.CLOSE_WAIT);
      setPacketInfo(p);

      TCPPacket ackPack = new TCPPacket(localport, port, seqNum, ackNum, true, false, false, windowSize, null);
      sendPacket(ackPack);
    }

    else if (p.finFlag && (currentState == State.FIN_WAIT_1)) {
      // go to closing
      changeStates(State.CLOSING);
      setPacketInfo(p);

      TCPPacket ackPack = new TCPPacket(localport, port, seqNum, ackNum, true, false, false, windowSize, null);
      sendPacket(ackPack);

    }

    else if (p.finFlag && (currentState == State.FIN_WAIT_2)) {
      // go to timewait
      changeStates(State.TIME_WAIT);
      setPacketInfo(p);

      TCPPacket ackPack = new TCPPacket(localport, port, seqNum, ackNum, true, false, false, windowSize, null);
      TCPWrapper.send(ackPack, address);

      createTimerTask(30 * 1000, null);
    }

    else if (p.ackFlag && (currentState == State.FIN_WAIT_1)) {
      // go to finwait2
      changeStates(State.FIN_WAIT_2);
    }

    else if (p.ackFlag && (currentState == State.CLOSING)) {
      // go to timewait
      changeStates(State.TIME_WAIT);
      createTimerTask(30 * 1000, null);
    }

    else if (p.ackFlag && (currentState == State.LAST_ACK)) {
      // go to timewait
      changeStates(State.TIME_WAIT);
      createTimerTask(30 * 1000, null);
    }

    this.notifyAll(); 
  }
  
  /** 
   * Waits for an incoming connection to arrive to connect this socket to
   * Ultimately this is called by the application calling 
   * ServerSocket.accept(), but this method belongs to the Socket object 
   * that will be returned, not the listening ServerSocket.
   * Note that localport is already set prior to this being called.
   */
  public synchronized void acceptConnection() throws IOException {
    // System.out.println("Register listen socket port is " + localport);
    D.registerListeningSocket(localport, this);

    changeStates(State.LISTEN);

    // wait until state has advanced to SYN_RCVD before returning 
    while (currentState != State.SYN_RCVD) {
      try {
        this.wait();
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  
  /**
   * Returns an input stream for this socket.  Note that this method cannot
   * create a NEW InputStream, but must return a reference to an 
   * existing InputStream (that you create elsewhere) because it may be
   * called more than once.
   *
   * @return     a stream for reading from this socket.
   * @exception  IOException  if an I/O error occurs when creating the
   *               input stream.
   */
  public InputStream getInputStream() throws IOException {
    // project 4 return appIS;
    return null;
    
  }

  /**
   * Returns an output stream for this socket.  Note that this method cannot
   * create a NEW InputStream, but must return a reference to an 
   * existing InputStream (that you create elsewhere) because it may be
   * called more than once.
   *
   * @return     an output stream for writing to this socket.
   * @exception  IOException  if an I/O error occurs when creating the
   *               output stream.
   */
  public OutputStream getOutputStream() throws IOException {
    // project 4 return appOS;
    return null;
  }


  /**
   * Closes this socket. 
   *
   * @exception  IOException  if an I/O error occurs when closing this socket.
   */
  public synchronized void close() throws IOException {

    // ServerSocket sock.close() does not have addr or port associated with it, so we can just return
    if (address == null || port == 0) {
        System.out.println("Server socket, returning early");
        return;
    }

    TCPPacket finPack = new TCPPacket(localport, port, seqNum, ackNum, false, false, true, windowSize, null);
    TCPWrapper.send(finPack, address);

    // sleep for a little to let packets between client and server transmitt fully
    try {
      Thread.sleep(10*500);
    }
    catch (Exception e) {
      e.printStackTrace();
    }


    if (currentState == State.ESTABLISHED) {
      changeStates(State.FIN_WAIT_1);
    }
    else if (currentState == State.CLOSE_WAIT) {
      changeStates(State.LAST_ACK);
    }

    // start background threads, let application close
    try {
      backgroundThread bgt = new backgroundThread(this);
      bgt.run();
    }
    catch( Exception e){
      e.printStackTrace();
    }    

  }

  /** 
   * create TCPTimerTask instance, handling tcpTimer creation
   * @param delay time in milliseconds before call
   * @param ref generic reference to be returned to handleTimer
   */
  private TCPTimerTask createTimerTask(long delay, Object ref){
    if(tcpTimer == null)
      tcpTimer = new Timer(false);
    return new TCPTimerTask(tcpTimer, delay, this, ref);
  }


  /**
   * handle timer expiration (called by TCPTimerTask)
   * @param ref Generic reference that can be used by the timer to return 
   * information.
   */
  public synchronized void handleTimer(Object ref){

    if (currentState == State.TIME_WAIT) {
      changeStates(State.CLOSED);
      // this must run only once the last timer (30 second timer) has expired
      tcpTimer.cancel();
      tcpTimer = null;
    }
    else {
      TCPPacket packetToResend = packetTable.get(currentState);
      sendPacket(packetToResend);
    }



      // try {
      //   D.unregisterConnection(address, localport, port, this);
      // }
      // catch (Exception e) {
      //   e.printStackTrace();
      // }
    
  }

  private synchronized void changeStates(State nextState) {
    System.out.println("!!! " + currentState + "->" + nextState);  
    currentState = nextState;
    this.notifyAll();
  }

  private synchronized void setPacketInfo(TCPPacket p) {
    address = p.sourceAddr;
    localport = p.destPort;
    port = p.sourcePort; // remoteport = p.sourceport
    int tempSeqNum = p.seqNum;
    seqNum = ackNum;
    ackNum = tempSeqNum + 1;
  }

  private synchronized void sendPacket(TCPPacket p) {
    TCPWrapper.send(p, address);
    TCPTimerTask timer = createTimerTask(30 * 1000, null);

    timerTable.put(currentState, timer);
    packetTable.put(currentState, p);
  }
}

// start new thread to cleanup rest of fin process
class backgroundThread implements Runnable{
  public StudentSocketImpl s;

  public backgroundThread (StudentSocketImpl s) throws InterruptedException{
    this.s = s;
  }

  @Override
  public void run() {
    while(s.currentState != StudentSocketImpl.State.CLOSED){
      try {
        s.wait();
      }
      catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
}
