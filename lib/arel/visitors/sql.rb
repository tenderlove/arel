module Arel
  module Visitors
    class Sql
      def initialize environment
        @dispatch_cache = {}
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

      def visit_Arel_Predicates_CompoundPredicate o
        # FIXME: remove the to_sql
        "(#{o.operand1.to_sql} #{o.predicate_sql} #{o.operand2})"
      end
      alias :visit_Arel_Predicates_Or :visit_Arel_Predicates_CompoundPredicate
      alias :visit_Arel_Predicates_And :visit_Arel_Predicates_CompoundPredicate

      def visit_Arel_Binary o
        sql = quote(o.operand2.value, o.operand1.column)
        "#{visit o.operand1} #{o.predicate_sql} #{sql}"
      end
      alias :visit_Arel_Predicates_Equality :visit_Arel_Binary

      def visit_Arel_Take o
        projections = o.projections
        if Count === projections.first && projections.size == 1 &&
          (o.taken.present? || o.wheres.present?) && o.joins(o).blank?

          @christener = o.relation.christener
          subquery = [
            "SELECT 1 FROM #{from_clauses o}", build_clauses(o)
          ].join ' '
          "SELECT COUNT(*) AS count_id FROM (#{subquery}) AS subquery"
        else
          visit_Arel_Relation o
        end
      end
      alias :visit_Arel_From :visit_Arel_Take
      alias :visit_Arel_Project :visit_Arel_Take

      def visit_Arel_Relation o
        @christener = o.relation.christener

        # FIXME: The AST is broken.  We need a SubProject or AliasProject
        # class to represent a Project that is aliased or a SubProject of a
        # project.
        selects = o.attributes.map do |attr|
          case attr
          when Project
            "(#{visit_Arel_Project(attr)}) AS #{quote_table_name(name_for(attr.table))}"
          else
            visit attr
          end
        end.join(', ')

        [
          "SELECT     #{selects}",
          "FROM       #{from_clauses o}",
          build_clauses(o)
        ].compact.join ' '
      end

      alias :visit_Arel_Alias :visit_Arel_Relation
      alias :visit_Arel_Group :visit_Arel_Relation
      alias :visit_Arel_Having :visit_Arel_Relation
      alias :visit_Arel_InnerJoin :visit_Arel_Relation
      alias :visit_Arel_Lock :visit_Arel_Relation
      alias :visit_Arel_Order :visit_Arel_Relation
      alias :visit_Arel_Skip :visit_Arel_Relation
      alias :visit_Arel_StringJoin :visit_Arel_Relation
      alias :visit_Arel_Table :visit_Arel_Relation
      alias :visit_Arel_Where :visit_Arel_Relation

      def visit_Arel_Expression o
        val = visit(o.attribute)

        "#{o.function_sql}(#{val})" +
          (o.alias ? " AS #{quote_column_name(o.alias)}" : " AS #{o.function_sql.to_s.downcase}_id")
      end
      alias :visit_Arel_Count :visit_Arel_Expression
      alias :visit_Arel_Sum :visit_Arel_Expression
      alias :visit_Arel_Average :visit_Arel_Expression
      alias :visit_Arel_Minimum :visit_Arel_Expression
      alias :visit_Arel_Maximum :visit_Arel_Expression

      def visit_Arel_Distinct o
        val = visit(o.attribute)
        "#{o.function_sql} #{val}" +
          (o.alias ? " AS #{quote_column_name(o.alias)}" : '')
      end

      def visit_Arel_Ordering o
        @christener = o.relation.christener
        "#{quote_table_name(name_for(o.attribute.original_relation))}.#{quote_column_name(o.attribute.name)} #{o.direction_sql}"
      end
      alias :visit_Arel_Ascending :visit_Arel_Ordering
      alias :visit_Arel_Descending :visit_Arel_Ordering

      def visit_Arel_Attribute o
        @christener = o.relation.christener
        "#{quote_table_name(name_for(o.original_relation))}.#{quote_column_name(o.name)}"
      end
      alias :visit_Arel_Sql_Attributes_Integer :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_String :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_Time :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_Boolean :visit_Arel_Attribute

      def visit_Arel_Value o
        o.value
      end

      def quote_table_name name
        @connection.quote_table_name name
      end

      def quote_column_name name
        @connection.quote_column_name name
      end

      def quote value, column = nil
        @connection.quote value, column
      end

      def name_for thing
        @christener.name_for thing
      end

      def visit object
        klass = object.class
        m = @dispatch_cache[klass] ||= :"visit_#{klass.name.gsub('::', '_')}"
        send m, object
      end

      def group_clauses o
        groups = o.groupings.map { |g| visit g }
        return if groups.empty?
        "GROUP BY  #{groups.join(', ')}"
      end

      def order_clauses o
        orders = o.orders.map { |thing| visit thing }
        return if orders.empty?
        "ORDER BY  #{orders.join(', ')}"
      end

      def having_clauses o
        havings = o.havings.map { |g| visit g }
        return if havings.empty?
        "HAVING    #{havings.join(' AND ')}"
      end

      def where_clauses o
        wheres = o.wheres.map { |w| visit w }
        return if wheres.empty?
        "WHERE     #{wheres.join(' AND ')}"
      end

      def build_clauses o
        joins   = o.joins(o)

        clauses = [ "",
          joins,
          where_clauses(o),
          group_clauses(o),
          having_clauses(o),
          order_clauses(o)
        ].compact.join ' '

        offset = o.skipped
        limit = o.taken
        @connection.add_limit_offset!(clauses, :limit => limit,
                                  :offset => offset) if offset || limit

        # FIXME: this needs to be in the adapter specific subclasses
        #clauses << " #{o.locked}" unless o.locked.blank?
        clauses unless clauses.blank?
      end

      def from_clauses o
        if o.sources.empty?
          table = o.table
          case table
          when Table
            table_name = table.name
            return table_name if table_name =~ /\s/

            unique_name = name_for(table)

            quote_table_name(table_name) +
              (table_name != unique_name ? " #{quote_table_name(unique_name)}" : '')
          else
            table.table_sql
          end
        else
          o.sources
        end
      end
    end
  end
end
