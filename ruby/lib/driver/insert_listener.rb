# frozen_string_literal: true

module River::Driver
  # Receives insert notifications on a dedicated Postgres connection. The
  # channel is qualified with the connection's schema, as the inserts are.
  class InsertListener
    def initialize(connection)
      @connection = connection
      schema = connection.exec("SELECT coalesce(current_schema(), '')").getvalue(0, 0)
      connection.exec("LISTEN #{connection.quote_ident("#{schema}.river_insert")}")
    end

    # Returns the payloads received within timeout seconds, or none when
    # interrupt becomes readable first. An idle wait pings the connection so a
    # dropped connection raises instead of waiting indefinitely.
    def wait(timeout, interrupt)
      payloads = received
      return payloads unless payloads.empty?

      # RBS can't infer IO.select's element types when some sets are nil.
      if IO.select([@connection.socket_io, interrupt], nil, nil, timeout) # steep:ignore
        @connection.consume_input
      else
        @connection.exec("SELECT 1")
      end
      received
    end

    private def received
      payloads = [] #: Array[String]
      while (notification = @connection.notifies)
        payloads << notification[:extra]
      end
      payloads
    end
  end
end
