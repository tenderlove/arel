module Arel
  module Visitors
    class Sql
      def initialize environment
        @environment = environment
        @engine      = environment.engine
        @christener  = nil
        @connection  = nil
      end

      def accept object
        method = :"visit_#{object.class.name.gsub('::', '_')}"
        send method, object
      end

      def visit_Arel_Attribute o
        sql = nil
        @engine.driver.connection_pool.with_connection do |connection|
          @connection = connection
          @christener = o.relation.christener
          sql = "#{quote_table_name(name_for(o.original_relation))}.#{quote_column_name(o.name)}"
        end
        sql
      end

      alias :visit_Arel_Sql_Attributes_Integer :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_String :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_Time :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_Boolean :visit_Arel_Attribute

      private
      def quote_table_name name
        @connection.quote_table_name name
      end

      def quote_column_name name
        @connection.quote_column_name name
      end

      def name_for thing
        @christener.name_for thing
      end
    end
  end
end
