module Sequel
  module Plugins
    # The PgTypecastOnLoad plugin exists because when you connect to PostgreSQL
    # using the do, swift, or jdbc adapter, Sequel doesn't have complete
    # control over typecasting, and may return columns as strings instead of how
    # the native postgres adapter would typecast them.  This is mostly needed for
    # the additional support that the pg_* extensions add for advanced PostgreSQL
    # types such as arrays.
    #
    # This plugin modifies Model#set_values to do the same conversion that the
    # native postgres adapter would do for all columns given.  You can either
    # specify the columns to typecast on load in the plugin call itself, or
    # afterwards using add_pg_typecast_on_load_columns:
    #
    #   # aliases => text[] column
    #   # config => hstore column
    #
    #   Album.plugin :pg_typecast_on_load, :aliases, :config
    #   # or:
    #   Album.plugin :pg_typecast_on_load
    #   Album.add_pg_typecast_on_load_columns :aliases, :config
    #
    # This plugin only handles values that the adapter returns as strings.  If
    # the adapter returns a value other than a string for a column, that value
    # will be used directly without typecasting.
    module PgTypecastOnLoad
      # Call add_pg_typecast_on_load_columns on the passed column arguments.
      def self.configure(model, *columns)
        model.instance_eval do
          @pg_typecast_on_load_columns ||= []
          add_pg_typecast_on_load_columns(*columns)
        end
      end

      module ClassMethods
        # The columns to typecast on load for this model.
        attr_reader :pg_typecast_on_load_columns

        # Add additional columns to typecast on load for this model.
        def add_pg_typecast_on_load_columns(*columns)
          @pg_typecast_on_load_columns.concat(columns)
        end

        # Give the subclass a copy of the columns to typecast on load.
        def inherited(subclass)
          super
          subclass.instance_variable_set(:@pg_typecast_on_load_columns, pg_typecast_on_load_columns.dup)
        end
      end

      module InstanceMethods
        # Lookup the conversion proc for the column's oid in the Database
        # object, and use it to convert the value.
        def set_values(values)
          model.pg_typecast_on_load_columns.each do |c|
            if (v = values[c]).is_a?(String) && (oid = db_schema[c][:oid])
              values[c] = db.conversion_procs[oid].call(v)
            end
          end
          super
        end
      end
    end
  end
end
