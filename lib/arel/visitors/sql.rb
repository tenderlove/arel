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
        @engine.driver.connection_pool.with_connection do |connection|
          @connection = connection

          visit object
        end
      end

      private

      def visit_Arel_Expression o
        # FIXME: remove this when we figure out how to visit a "Value"
        val = Value === o.attribute ? o.attribute.value : visit(o.attribute)

        "#{o.function_sql}(#{val})" +
          (o.alias ? " AS #{quote_column_name(o.alias)}" : " AS #{o.function_sql.to_s.downcase}_id")
      end
      alias :visit_Arel_Count :visit_Arel_Expression
      alias :visit_Arel_Sum :visit_Arel_Expression
      alias :visit_Arel_Average :visit_Arel_Expression
      alias :visit_Arel_Minimum :visit_Arel_Expression
      alias :visit_Arel_Maximum :visit_Arel_Expression

      def visit_Arel_Distinct o
        # FIXME: remove this when we figure out how to visit a "Value"
        val = Value === o.attribute ? o.attribute.value : visit(o.attribute)
        "#{o.function_sql} #{val}" +
          (o.alias ? " AS #{quote_column_name(o.alias)}" : '')
      end

      def visit_Arel_Ordering o
        "#{quote_table_name(name_for(o.attribute.original_relation))}.#{quote_column_name(o.attribute.name)} #{o.direction_sql}"
      end
      alias :visit_Arel_Ascending :visit_Arel_Ordering
      alias :visit_Arel_Descending :visit_Arel_Ordering

      def visit_Arel_Attribute o
        "#{quote_table_name(name_for(o.original_relation))}.#{quote_column_name(o.name)}"
      end
      alias :visit_Arel_Sql_Attributes_Integer :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_String :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_Time :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_Boolean :visit_Arel_Attribute

      def quote_table_name name
        @connection.quote_table_name name
      end

      def quote_column_name name
        @connection.quote_column_name name
      end

      def name_for thing
        @christener.name_for thing
      end

      def visit object
        @christener = object.relation.christener
        method      = :"visit_#{object.class.name.gsub('::', '_')}"

        send method, object
      end
    end
  end
end
