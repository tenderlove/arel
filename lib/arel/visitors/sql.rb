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

      def visit_Arel_Project o
        projections = o.projections
        if Count === projections.first && projections.size == 1 &&
          (o.taken.present? || o.wheres.present?) && o.joins(self).blank?
          subquery = [
            "SELECT 1 FROM #{o.from_clauses}", build_clauses(o)
          ].join ' '
          query = "SELECT COUNT(*) AS count_id FROM (#{subquery}) AS subquery"
        else
          query = [
            "SELECT     #{o.select_clauses.join(', ')}",
            "FROM       #{o.from_clauses}",
            build_clauses(o)
          ].compact.join ' '
        end
        query
      end
      alias :visit_Arel_Table :visit_Arel_Project
      alias :visit_Arel_Where :visit_Arel_Project
      alias :visit_Arel_Take :visit_Arel_Project
      alias :visit_Arel_Skip :visit_Arel_Project
      alias :visit_Arel_Order :visit_Arel_Project
      alias :visit_Arel_Lock :visit_Arel_Project
      alias :visit_Arel_StringJoin :visit_Arel_Project
      alias :visit_Arel_InnerJoin :visit_Arel_Project
      alias :visit_Arel_Having :visit_Arel_Project
      alias :visit_Arel_Group :visit_Arel_Project
      alias :visit_Arel_From :visit_Arel_Project
      alias :visit_Arel_Alias :visit_Arel_Project

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

      def build_clauses o
        joins   = o.joins(o)
        wheres  = o.where_clauses
        groups  = o.group_clauses
        havings = o.having_clauses
        orders  = o.order_clauses

        clauses = [ "",
          joins,
          ("WHERE     #{wheres.join(' AND ')}" unless wheres.empty?),
          ("GROUP BY  #{groups.join(', ')}" unless groups.empty?),
          ("HAVING    #{havings.join(' AND ')}" unless havings.empty?),
          ("ORDER BY  #{orders.join(', ')}" unless orders.empty?)
        ].compact.join ' '

        offset = o.skipped
        limit = o.taken
        @connection.add_limit_offset!(clauses, :limit => limit,
                                  :offset => offset) if offset || limit

        # FIXME: this needs to be in the adapter specific subclasses
        #clauses << " #{o.locked}" unless o.locked.blank?
        clauses unless clauses.blank?
      end
    end
  end
end
