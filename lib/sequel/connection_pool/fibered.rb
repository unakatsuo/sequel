require 'fiber'

# A fibered version of ThreadedConnectionPool. It allows multi-fibered
# access to a pool of connections.
# Based on ideas from EM::Synchrony::ConnectionPool.
class Sequel::FiberedConnectionPool < Sequel::ConnectionPool
  # The maximum number of connections this pool will create (per shard/server
  # if sharding).
  attr_reader :max_size
  
  # An array of connections that are available for use by the pool.
  attr_reader :available_connections
  
  # A hash with fiber keys and connection values for currently allocated
  # connections.
  attr_reader :allocated

  # A queue for fibers that are waiting for connections.
  attr_reader :pending

  # The following additional options are respected:
  # * :max_connections - The maximum number of connections the connection pool
  #   will open (default 4)
  def initialize(opts = {}, &block)
    super
    @max_size = Integer(opts[:max_connections] || 4)
    raise(Sequel::Error, ':max_connections must be positive') if @max_size < 1
    @available_connections = []
    @pending = []
    @allocated = {}
    @preallocated = {}
    # TODO: pool_timeout support
    @timeout = Integer(opts[:pool_timeout] || 5)
  end
  
  # The total number of connections opened for the given server.
  def size
    @allocated.length + @available_connections.length + @preallocated.length
  end
  
  # Removes all connections currently available on all servers, optionally
  # yielding each connection to the given block. This method has the effect of 
  # disconnecting from the database, assuming that no connections are currently
  # being used.
  # 
  # Once a connection is requested using #hold, the connection pool
  # creates new connections to the database.
  def disconnect(opts={}, &block)
    block ||= @disconnection_proc
    @available_connections.each{|conn| block.call(conn)} if block
    @available_connections.clear
  end
  
  # Chooses the first available connection to the DEFAULT_SERVER. If none are
  # available, creates a new connection (< max_size) or pushes to the
  # pending (defer) queue (>= max_size). Those defered requests are
  # processed at every connection release event until the queue gets empty.
  #
  # Usage is exactly same as ThreadedConnectionPool does:
  # 
  #   pool.hold {|conn| conn.execute('DROP TABLE posts')}
  # 
  # This hold() is re-entrant within same fiber context as same as
  # ThreadedConnectionPool. It simply yields the given block when this
  # is called recursively.
  #
  # Note that PoolTimeout error is not handled.
  def hold(server=nil)
    f = Fiber.current
    
    return yield(conn) if conn = owned_connection(f)
    begin
      conn = acquire(f)
      yield conn
    rescue Sequel::DatabaseDisconnectError
      oconn = conn
      conn = nil
      @disconnection_proc.call(oconn) if @disconnection_proc && oconn
      @allocated.delete(f)
      raise
    ensure
      release(f) if conn
    end
  end
  
  private

  # Assigns a connection to the supplied thread for the DEFAULT_SERVER, if one
  # is available. 
  def acquire(fiber)
    if conn = @available_connections.pop
      @allocated[fiber] = conn
    else
      if size < @max_size
        @preallocated[fiber] = 1
        conn = @allocated[fiber] = make_new(DEFAULT_SERVER)
        @preallocated.delete(fiber)
        conn
      else
        Fiber.yield @pending.push(fiber)
        acquire(fiber)
      end
    end
  end
  
  # Returns the connection owned by the supplied fiber.
  def owned_connection(fiber)
    @allocated[fiber]
  end
  
  # Releases the connection assigned to the supplied fiber. If the
  # server or connection given is scheduled for disconnection, remove the
  # connection instead of releasing it back to the pool.
  def release(fiber)
    @available_connections << @allocated.delete(fiber)
    if pending = @pending.shift
      pending.resume
    end
  end

  CONNECTION_POOL_MAP[[:fiber, false]] = self
end
