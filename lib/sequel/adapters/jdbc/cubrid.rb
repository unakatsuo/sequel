Sequel.require 'adapters/shared/cubrid'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    module Cubrid
      module DatabaseMethods
        include Sequel::Cubrid::DatabaseMethods
        include Sequel::JDBC::Transactions

        def supports_savepoints?
          false
        end
      
        private
        
        # Get the last inserted id using LAST_INSERT_ID().
        def last_insert_id(conn, opts={})
          if stmt = opts[:stmt]
            rs = stmt.getGeneratedKeys
            begin
              if rs.next
                rs.getInt(1)
              end
            rescue NativeException
              nil
            ensure
              rs.close
            end
          end
        end

        # Use execute instead of executeUpdate.
        def execute_prepared_statement_insert(stmt)
          stmt.execute
        end
      
        # Return generated keys for insert statements, and use
        # execute intead of executeUpdate as CUBRID doesn't
        # return generated keys in executeUpdate.
        def execute_statement_insert(stmt, sql)
          stmt.execute(sql, JavaSQL::Statement.RETURN_GENERATED_KEYS)
        end

        # Return generated keys for insert statements.
        def prepare_jdbc_statement(conn, sql, opts)
          opts[:type] == :insert ? conn.prepareStatement(sql, JavaSQL::Statement.RETURN_GENERATED_KEYS) : super
        end
      end
    end
  end
end
