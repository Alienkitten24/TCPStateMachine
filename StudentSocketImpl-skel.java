import java.net.*;
import java.io.*;
import java.util.Timer;

class StudentSocketImpl extends BaseSocketImpl {

  // SocketImpl data members:
  //   protected InetAddress address;
  //   protected int port;
  //   protected int localport;

  private Demultiplexer D;
  private Timer tcpTimer;

  private enum State {
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

  private State currentState = State.CLOSED;
  private static int initSeqNum = 100;
  private static int initAckNum = 150;
  private int seqNum = initSeqNum; 
  private int ackNum = initAckNum;
  private int windowSize = 1;
  private boolean isProcessingPacketFlag = false;


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
    System.out.println("Connect() register connection is " + address + " " + localport + " " + port);
    D.registerConnection(address, localport, port, this);

    // TODO keep an eye on changing initseqnum -> seqnum
    TCPPacket synPack = new TCPPacket(localport, port, seqNum, 0, false, true, false, windowSize, null);
    TCPWrapper.send(synPack, address);

    changeStates(State.SYN_SENT);

    // wait until state has advanced to ESTABLISHED before returning 
    while (currentState != State.ESTABLISHED) {
      try {
        this.wait();
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // System.out.println("YOPIERRE");
  }
  
  /**
   * Called by Demultiplexer when a packet comes in for this connection
   * @param p The packet that arrived
   */
  public synchronized void receivePacket(TCPPacket p){
    isProcessingPacketFlag = true;

    System.out.println("BIRTHOFRAP");
    System.out.println(p.getDebugOutput());
    System.out.flush();

    if (p.synFlag && !p.ackFlag) {
      System.out.println("SYN YESSIR");

      setPacketInfo(p);

      // TODO prob need to double chech this
      try {
        D.unregisterListeningSocket(localport, this);
        // System.out.println("SYN() register connection is " + address + " " + localport + " " + port + " " + this);
        // System.out.println(address);
        D.registerConnection(address, localport, port, this);
      }
      catch (IOException e) {
        e.printStackTrace();
      }

      TCPPacket synAckPack = new TCPPacket(localport, port, seqNum, ackNum, true, true, false, windowSize, null);
      TCPWrapper.send(synAckPack, address);

      changeStates(State.SYN_RCVD);
    }

    else if (p.synFlag && p.ackFlag) {
      System.out.println("SYNACK YESSIR");

      setPacketInfo(p);

      TCPPacket ackPack = new TCPPacket(localport, port, seqNum, ackNum, true, false, false, windowSize, null);
      TCPWrapper.send(ackPack, address);

      changeStates(State.ESTABLISHED);
    }

    else if (!p.synFlag && p.ackFlag) {
      System.out.println("ACK YESSIR");
 
      changeStates(State.ESTABLISHED);
    }

    else if (p.finFlag && (currentState == State.ESTABLISHED)) {
      // go to closewait
      setPacketInfo(p);

      TCPPacket ackPack = new TCPPacket(localport, port, seqNum, ackNum, true, false, false, windowSize, null);
      TCPWrapper.send(ackPack, address);

      changeStates(State.CLOSE_WAIT);
    }

    else if (p.finFlag && (currentState == State.FIN_WAIT_1)) {
      // go to closing

      setPacketInfo(p);

      TCPPacket ackPack = new TCPPacket(localport, port, seqNum, ackNum, true, false, false, windowSize, null);
      TCPWrapper.send(ackPack, address);

      changeStates(State.CLOSING);
    }

    else if (p.finFlag && (currentState == State.FIN_WAIT_2)) {
      // go to timewait
      setPacketInfo(p);

      TCPPacket ackPack = new TCPPacket(localport, port, seqNum, ackNum, true, false, false, windowSize, null);
      TCPWrapper.send(ackPack, address);

      changeStates(State.TIME_WAIT);
    }

    else if (p.ackFlag && (currentState == State.FIN_WAIT_1)) {
      // go to finwait2
      changeStates(State.FIN_WAIT_2);
    }

    else if (p.ackFlag && (currentState == State.CLOSING)) {
      // go to timewait
      changeStates(State.TIME_WAIT);
    }

    else if (p.ackFlag && (currentState == State.LAST_ACK)) {
      // go to timewait
      changeStates(State.TIME_WAIT);
    }

    isProcessingPacketFlag = false;
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
    while (isProcessingPacketFlag) {
      System.out.println("FLAG");
      try {
        this.wait(); // wait for recievepacket to finish so that currentState is correct
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // wrong since seqnum is out of sync with p.seqNum -- this might be fixed now ?
    int tempSeqNum = seqNum;
    seqNum = ackNum;
    ackNum = tempSeqNum + 1;
    // int windowSize = 1; 

    System.out.println("CLOSING " + address + " " + localport + " " + port + " ");

    TCPPacket finPack = new TCPPacket(localport, port, seqNum, ackNum, false, false, true, windowSize, null);
    TCPWrapper.send(finPack, address);

    if (currentState == State.ESTABLISHED) {
      changeStates(State.FIN_WAIT_1);
    }
    else if (currentState == State.CLOSE_WAIT) {
      changeStates(State.LAST_ACK);
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

    // this must run only once the last timer (30 second timer) has expired
    tcpTimer.cancel();
    tcpTimer = null;
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
    System.out.println("Changing Info " + address + " " + localport + " " + port);
    System.out.flush();
  }
}
