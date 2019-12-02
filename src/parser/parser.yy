%language "C++"
%skeleton "lalr1.cc"
%no-lines
%locations
%define api.namespace { nebula }
%define parser_class_name { GraphParser }
%lex-param { nebula::GraphScanner& scanner }
%parse-param { nebula::GraphScanner& scanner }
%parse-param { std::string &errmsg }
%parse-param { nebula::SequentialSentences** sentences }

%code requires {
#include <iostream>
#include <sstream>
#include <string>
#include "parser/SequentialSentences.h"

namespace nebula {

class GraphScanner;

}

}

%code {
    static int yylex(nebula::GraphParser::semantic_type* yylval,
                     nebula::GraphParser::location_type *yylloc,
                     nebula::GraphScanner& scanner);
}

%union {
    bool                                    boolval;
    int64_t                                 intval;
    double                                  doubleval;
    std::string                            *strval;
    nebula::Expression                     *expr;
    nebula::Sentence                       *sentence;
    nebula::SequentialSentences            *sentences;
    nebula::ColumnSpecification            *colspec;
    nebula::ColumnSpecificationList        *colspeclist;
    nebula::ColumnNameList                 *colsnamelist;
    nebula::ColumnType                      type;
    nebula::StepClause                     *step_clause;
    nebula::StepClause                     *find_path_upto_clause;
    nebula::FromClause                     *from_clause;
    nebula::ToClause                       *to_clause;
    nebula::VertexIDList                   *vid_list;
    nebula::OverEdge                       *over_edge;
    nebula::OverEdges                      *over_edges;
    nebula::OverClause                     *over_clause;
    nebula::WhereClause                    *where_clause;
    nebula::WhenClause                     *when_clause;
    nebula::YieldClause                    *yield_clause;
    nebula::YieldColumns                   *yield_columns;
    nebula::YieldColumn                    *yield_column;
    nebula::VertexTagList                  *vertex_tag_list;
    nebula::VertexTagItem                  *vertex_tag_item;
    nebula::PropertyList                   *prop_list;
    nebula::ValueList                      *value_list;
    nebula::VertexRowList                  *vertex_row_list;
    nebula::VertexRowItem                  *vertex_row_item;
    nebula::EdgeRowList                    *edge_row_list;
    nebula::EdgeRowItem                    *edge_row_item;
    nebula::UpdateList                     *update_list;
    nebula::UpdateItem                     *update_item;
    nebula::EdgeList                       *edge_list;
    nebula::ArgumentList                   *argument_list;
    nebula::SpaceOptList                   *space_opt_list;
    nebula::SpaceOptItem                   *space_opt_item;
    nebula::AlterSchemaOptList             *alter_schema_opt_list;
    nebula::AlterSchemaOptItem             *alter_schema_opt_item;
    nebula::WithUserOptList                *with_user_opt_list;
    nebula::WithUserOptItem                *with_user_opt_item;
    nebula::RoleTypeClause                 *role_type_clause;
    nebula::AclItemClause                  *acl_item_clause;
    nebula::SchemaPropList                 *create_schema_prop_list;
    nebula::SchemaPropItem                 *create_schema_prop_item;
    nebula::SchemaPropList                 *alter_schema_prop_list;
    nebula::SchemaPropItem                 *alter_schema_prop_item;
    nebula::OrderFactor                    *order_factor;
    nebula::OrderFactors                   *order_factors;
    nebula::ConfigModule                    config_module;
    nebula::ConfigRowItem                  *config_row_item;
    nebula::EdgeKey                        *edge_key;
    nebula::EdgeKeys                       *edge_keys;
    nebula::EdgeKeyRef                     *edge_key_ref;
    nebula::GroupClause                    *group_clause;
    nebula::HostList                       *host_list;
    nebula::HostAddr                       *host_item;
}

/* destructors */
%destructor {} <sentences>
%destructor {} <boolval> <intval> <doubleval> <type> <config_module>
%destructor { delete $$; } <*>

/* keywords */
%token KW_GO KW_AS KW_TO KW_OR KW_AND KW_XOR KW_USE KW_SET KW_FROM KW_WHERE KW_ALTER
%token KW_MATCH KW_INSERT KW_VALUES KW_YIELD KW_RETURN KW_CREATE KW_VERTEX
%token KW_EDGE KW_EDGES KW_STEPS KW_OVER KW_UPTO KW_REVERSELY KW_SPACE KW_DELETE KW_FIND
%token KW_INT KW_BIGINT KW_DOUBLE KW_STRING KW_BOOL KW_TAG KW_TAGS KW_UNION KW_INTERSECT KW_MINUS
%token KW_NO KW_OVERWRITE KW_IN KW_DESCRIBE KW_DESC KW_SHOW KW_HOSTS KW_PARTS KW_TIMESTAMP KW_ADD
%token KW_PARTITION_NUM KW_REPLICA_FACTOR KW_DROP KW_REMOVE KW_SPACES KW_INGEST KW_UUID
%token KW_IF KW_NOT KW_EXISTS KW_WITH KW_FIRSTNAME KW_LASTNAME KW_EMAIL KW_PHONE KW_USER KW_USERS
%token KW_PASSWORD KW_CHANGE KW_ROLE KW_GOD KW_ADMIN KW_GUEST KW_GRANT KW_REVOKE KW_ON
%token KW_ROLES KW_BY KW_DOWNLOAD KW_HDFS
%token KW_CONFIGS KW_GET KW_DECLARE KW_GRAPH KW_META KW_STORAGE
%token KW_TTL_DURATION KW_TTL_COL KW_DEFAULT
%token KW_ORDER KW_ASC KW_LIMIT KW_OFFSET KW_GROUP
%token KW_COUNT KW_COUNT_DISTINCT KW_SUM KW_AVG KW_MAX KW_MIN KW_STD KW_BIT_AND KW_BIT_OR KW_BIT_XOR
%token KW_FETCH KW_PROP KW_UPDATE KW_UPSERT KW_WHEN
%token KW_DISTINCT KW_ALL KW_OF
%token KW_BALANCE KW_LEADER KW_DATA KW_STOP
%token KW_SHORTEST KW_PATH
%token KW_IS KW_NULL

/* symbols */
%token L_PAREN R_PAREN L_BRACKET R_BRACKET L_BRACE R_BRACE COMMA
%token PIPE OR AND XOR LT LE GT GE EQ NE PLUS MINUS MUL DIV MOD NOT NEG ASSIGN
%token DOT COLON SEMICOLON L_ARROW R_ARROW AT
%token ID_PROP TYPE_PROP SRC_ID_PROP DST_ID_PROP RANK_PROP INPUT_REF DST_REF SRC_REF

/* token type specification */
%token <boolval> BOOL
%token <intval> INTEGER IPV4
%token <doubleval> DOUBLE
%token <strval> STRING VARIABLE LABEL

%type <strval> name_label unreserved_keyword agg_function
%type <expr> expression logic_xor_expression logic_or_expression logic_and_expression
%type <expr> relational_expression multiplicative_expression additive_expression arithmetic_xor_expression
%type <expr> unary_expression primary_expression equality_expression
%type <expr> src_ref_expression
%type <expr> dst_ref_expression
%type <expr> input_ref_expression
%type <expr> var_ref_expression
%type <expr> alias_ref_expression
%type <expr> vid_ref_expression
%type <expr> vid
%type <expr> function_call_expression
%type <expr> uuid_expression
%type <argument_list> argument_list opt_argument_list
%type <type> type_spec
%type <step_clause> step_clause
%type <from_clause> from_clause
%type <vid_list> vid_list
%type <over_edge> over_edge
%type <over_edges> over_edges
%type <over_clause> over_clause
%type <where_clause> where_clause
%type <when_clause> when_clause
%type <yield_clause> yield_clause
%type <yield_columns> yield_columns
%type <yield_column> yield_column
%type <vertex_tag_list> vertex_tag_list
%type <vertex_tag_item> vertex_tag_item
%type <prop_list> prop_list
%type <value_list> value_list
%type <vertex_row_list> vertex_row_list
%type <vertex_row_item> vertex_row_item
%type <edge_row_list> edge_row_list
%type <edge_row_item> edge_row_item
%type <update_list> update_list
%type <update_item> update_item
%type <edge_list> edge_list
%type <space_opt_list> space_opt_list
%type <space_opt_item> space_opt_item
%type <alter_schema_opt_list> alter_schema_opt_list
%type <alter_schema_opt_item> alter_schema_opt_item
%type <create_schema_prop_list> create_schema_prop_list opt_create_schema_prop_list
%type <create_schema_prop_item> create_schema_prop_item
%type <alter_schema_prop_list> alter_schema_prop_list
%type <alter_schema_prop_item> alter_schema_prop_item
%type <order_factor> order_factor
%type <order_factors> order_factors
%type <config_module> config_module_enum
%type <config_row_item> show_config_item get_config_item set_config_item
%type <edge_key> edge_key
%type <edge_keys> edge_keys
%type <edge_key_ref> edge_key_ref
%type <to_clause> to_clause
%type <find_path_upto_clause> find_path_upto_clause
%type <group_clause> group_clause
%type <host_list> host_list
%type <host_item> host_item

%type <intval> unary_integer rank port

%type <colspec> column_spec
%type <colspeclist> column_spec_list
%type <colsnamelist> column_name_list

%type <with_user_opt_list> with_user_opt_list
%type <with_user_opt_item> with_user_opt_item
%type <role_type_clause> role_type_clause
%type <acl_item_clause> acl_item_clause

%type <sentence> go_sentence match_sentence use_sentence find_sentence find_path_sentence
%type <sentence> order_by_sentence limit_sentence group_by_sentence
%type <sentence> fetch_vertices_sentence fetch_edges_sentence
%type <sentence> create_tag_sentence create_edge_sentence
%type <sentence> alter_tag_sentence alter_edge_sentence
%type <sentence> describe_tag_sentence describe_edge_sentence
%type <sentence> drop_tag_sentence drop_edge_sentence
%type <sentence> traverse_sentence set_sentence piped_sentence assignment_sentence fetch_sentence
%type <sentence> maintain_sentence insert_vertex_sentence insert_edge_sentence
%type <sentence> mutate_sentence update_vertex_sentence update_edge_sentence delete_vertex_sentence delete_edge_sentence
%type <sentence> ingest_sentence
%type <sentence> show_sentence create_space_sentence describe_space_sentence
%type <sentence> drop_space_sentence
%type <sentence> yield_sentence
%type <sentence> create_user_sentence alter_user_sentence drop_user_sentence change_password_sentence
%type <sentence> grant_sentence revoke_sentence
%type <sentence> download_sentence
%type <sentence> set_config_sentence get_config_sentence balance_sentence
%type <sentence> process_control_sentence return_sentence
%type <sentence> sentence
%type <sentences> sentences

%start sentences

%%

name_label
     : LABEL { $$ = $1; }
     | unreserved_keyword { $$ = $1; }
     ;

unreserved_keyword
     : KW_SPACE              { $$ = new std::string("space"); }
     | KW_VALUES             { $$ = new std::string("values"); }
     | KW_HOSTS              { $$ = new std::string("hosts"); }
     | KW_SPACES             { $$ = new std::string("spaces"); }
     | KW_FIRSTNAME          { $$ = new std::string("firstname"); }
     | KW_LASTNAME           { $$ = new std::string("lastname"); }
     | KW_EMAIL              { $$ = new std::string("email"); }
     | KW_PHONE              { $$ = new std::string("phone"); }
     | KW_USER               { $$ = new std::string("user"); }
     | KW_USERS              { $$ = new std::string("users"); }
     | KW_PASSWORD           { $$ = new std::string("password"); }
     | KW_ROLE               { $$ = new std::string("role"); }
     | KW_ROLES              { $$ = new std::string("roles"); }
     | KW_GOD                { $$ = new std::string("god"); }
     | KW_ADMIN              { $$ = new std::string("admin"); }
     | KW_GUEST              { $$ = new std::string("guest"); }
     | KW_GROUP              { $$ = new std::string("group"); }
     | KW_COUNT              { $$ = new std::string("count"); }
     | KW_SUM                { $$ = new std::string("sum"); }
     | KW_AVG                { $$ = new std::string("avg"); }
     | KW_MAX                { $$ = new std::string("max"); }
     | KW_MIN                { $$ = new std::string("min"); }
     | KW_STD                { $$ = new std::string("std"); }
     | KW_BIT_AND            { $$ = new std::string("bit_and"); }
     | KW_BIT_OR             { $$ = new std::string("bit_or"); }
     | KW_BIT_XOR            { $$ = new std::string("bit_xor"); }
     ;

agg_function
     : KW_COUNT              { $$ = new std::string("COUNT"); }
     | KW_COUNT_DISTINCT     { $$ = new std::string("COUNT_DISTINCT"); }
     | KW_SUM                { $$ = new std::string("SUM"); }
     | KW_AVG                { $$ = new std::string("AVG"); }
     | KW_MAX                { $$ = new std::string("MAX"); }
     | KW_MIN                { $$ = new std::string("MIN"); }
     | KW_STD                { $$ = new std::string("STD"); }
     | KW_BIT_AND            { $$ = new std::string("BIT_AND"); }
     | KW_BIT_OR             { $$ = new std::string("BIT_OR"); }
     | KW_BIT_XOR            { $$ = new std::string("BIT_XOR"); }
     ;

primary_expression
    : INTEGER {
        $$ = new PrimaryExpression($1);
    }
    | DOUBLE {
        $$ = new PrimaryExpression($1);
    }
    | STRING {
        $$ = new PrimaryExpression(*$1);
        delete $1;
    }
    | BOOL {
        $$ = new PrimaryExpression($1);
    }
    | name_label {
        $$ = new PrimaryExpression(*$1);
        delete $1;
    }
    | input_ref_expression {
        $$ = $1;
    }
    | src_ref_expression {
        $$ = $1;
    }
    | dst_ref_expression {
        $$ = $1;
    }
    | var_ref_expression {
        $$ = $1;
    }
    | alias_ref_expression {
        $$ = $1;
    }
    | L_PAREN expression R_PAREN {
        $$ = $2;
    }
    | function_call_expression {
        $$ = $1;
    }
    ;

input_ref_expression
    : INPUT_REF DOT name_label {
        $$ = new InputPropertyExpression($3);
    }
    | INPUT_REF {
        // To reference the `id' column implicitly
        $$ = new InputPropertyExpression(new std::string("id"));
    }
    | INPUT_REF DOT MUL {
        $$ = new InputPropertyExpression(new std::string("*"));
    }
    ;

src_ref_expression
    : SRC_REF DOT name_label DOT name_label {
        $$ = new SourcePropertyExpression($3, $5);
    }
    ;

dst_ref_expression
    : DST_REF DOT name_label DOT name_label {
        $$ = new DestPropertyExpression($3, $5);
    }
    ;

var_ref_expression
    : VARIABLE DOT name_label {
        $$ = new VariablePropertyExpression($1, $3);
    }
    | VARIABLE {
        $$ = new VariablePropertyExpression($1, new std::string("id"));
    }
    | VARIABLE DOT MUL {
        $$ = new VariablePropertyExpression($1, new std::string("*"));
    }
    ;

alias_ref_expression
    : name_label DOT name_label {
        $$ = new AliasPropertyExpression(new std::string(""), $1, $3);
    }
    | name_label DOT TYPE_PROP {
        $$ = new EdgeTypeExpression($1);
    }
    | name_label DOT SRC_ID_PROP {
        $$ = new EdgeSrcIdExpression($1);
    }
    | name_label DOT DST_ID_PROP {
        $$ = new EdgeDstIdExpression($1);
    }
    | name_label DOT RANK_PROP {
        $$ = new EdgeRankExpression($1);
    }
    ;

function_call_expression
    : LABEL L_PAREN opt_argument_list R_PAREN {
        $$ = new FunctionCallExpression($1, $3);
    }
    ;

uuid_expression
    : KW_UUID L_PAREN STRING R_PAREN {
        $$ = new UUIDExpression($3);
    }
    ;

opt_argument_list
    : %empty {
        $$ = nullptr;
    }
    | argument_list {
        $$ = $1;
    }
    ;

argument_list
    : expression {
        $$ = new ArgumentList();
        $$->addArgument($1);
    }
    | argument_list COMMA expression {
        $$ = $1;
        $$->addArgument($3);
    }
    ;

unary_expression
    : primary_expression { $$ = $1; }
    | PLUS unary_expression {
        $$ = new UnaryExpression(UnaryExpression::PLUS, $2);
    }
    | MINUS unary_expression {
        $$ = new UnaryExpression(UnaryExpression::NEGATE, $2);
    }
    | NOT unary_expression {
        $$ = new UnaryExpression(UnaryExpression::NOT, $2);
    }
    | KW_NOT unary_expression {
        $$ = new UnaryExpression(UnaryExpression::NOT, $2);
    }
    | L_PAREN type_spec R_PAREN unary_expression {
        $$ = new TypeCastingExpression($2, $4);
    }
    ;

type_spec
    : KW_INT { $$ = ColumnType::INT; }
    | KW_DOUBLE { $$ = ColumnType::DOUBLE; }
    | KW_STRING { $$ = ColumnType::STRING; }
    | KW_BOOL { $$ = ColumnType::BOOL; }
    | KW_BIGINT { $$ = ColumnType::BIGINT; }
    | KW_TIMESTAMP { $$ = ColumnType::TIMESTAMP; }
    ;

arithmetic_xor_expression
    : unary_expression { $$ = $1; }
    | arithmetic_xor_expression XOR unary_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::XOR, $3);
    }
    ;

multiplicative_expression
    : arithmetic_xor_expression { $$ = $1; }
    | multiplicative_expression MUL arithmetic_xor_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::MUL, $3);
    }
    | multiplicative_expression DIV arithmetic_xor_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::DIV, $3);
    }
    | multiplicative_expression MOD arithmetic_xor_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::MOD, $3);
    }
    ;

additive_expression
    : multiplicative_expression { $$ = $1; }
    | additive_expression PLUS multiplicative_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::ADD, $3);
    }
    | additive_expression MINUS multiplicative_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::SUB, $3);
    }
    ;

relational_expression
    : additive_expression { $$ = $1; }
    | relational_expression LT additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::LT, $3);
    }
    | relational_expression GT additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::GT, $3);
    }
    | relational_expression LE additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::LE, $3);
    }
    | relational_expression GE additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::GE, $3);
    }
    ;

equality_expression
    : relational_expression { $$ = $1; }
    | equality_expression EQ relational_expression {
        $$ = new RelationalExpression($1, RelationalExpression::EQ, $3);
    }
    | equality_expression NE relational_expression {
        $$ = new RelationalExpression($1, RelationalExpression::NE, $3);
    }
    ;

logic_and_expression
    : equality_expression { $$ = $1; }
    | logic_and_expression AND equality_expression {
        $$ = new LogicalExpression($1, LogicalExpression::AND, $3);
    }
    | logic_and_expression KW_AND equality_expression {
        $$ = new LogicalExpression($1, LogicalExpression::AND, $3);
    }
    ;

logic_or_expression
    : logic_and_expression { $$ = $1; }
    | logic_or_expression OR logic_and_expression {
        $$ = new LogicalExpression($1, LogicalExpression::OR, $3);
    }
    | logic_or_expression KW_OR logic_and_expression {
        $$ = new LogicalExpression($1, LogicalExpression::OR, $3);
    }
    ;

logic_xor_expression
    : logic_or_expression { $$ = $1; }
    | logic_xor_expression KW_XOR logic_or_expression {
        $$ = new LogicalExpression($1, LogicalExpression::XOR, $3);
    }
    ;

expression
    : logic_xor_expression { $$ = $1; }
    ;

go_sentence
    : KW_GO step_clause from_clause over_clause where_clause yield_clause {
        auto go = new GoSentence();
        go->setStepClause($2);
        go->setFromClause($3);
        go->setOverClause($4);
        go->setWhereClause($5);
        if ($6 == nullptr) {
            auto *cols = new YieldColumns();
            for (auto e : $4->edges()) {
                if (e->isOverAll()) {
                    continue;
                }
                auto *edge  = new std::string(*e->edge());
                auto *expr  = new EdgeDstIdExpression(edge);
                auto *col   = new YieldColumn(expr);
                cols->addColumn(col);
            }
            $6 = new YieldClause(cols);
        }
        go->setYieldClause($6);
        $$ = go;
    }
    ;

step_clause
    : %empty { $$ = new StepClause(); }
    | INTEGER KW_STEPS { $$ = new StepClause($1); }
    | KW_UPTO INTEGER KW_STEPS { $$ = new StepClause($2, true); }
    ;

from_clause
    : KW_FROM vid_list {
        $$ = new FromClause($2);
    }
    | KW_FROM vid_ref_expression {
        $$ = new FromClause($2);
    }
    ;

vid_list
    : vid {
        $$ = new VertexIDList();
        $$->add($1);
    }
    | vid_list COMMA vid {
        $$ = $1;
        $$->add($3);
    }
    ;

vid
    : unary_integer {
        $$ = new PrimaryExpression($1);
    }
    | function_call_expression {
        $$ = $1;
    }
    | uuid_expression {
        $$ = $1;
    }
    ;

unary_integer
    : PLUS INTEGER {
        $$ = $2;
    }
    | MINUS INTEGER {
        $$ = -$2;
    }
    | INTEGER {
        $$ = $1;
    }
    ;

vid_ref_expression
    : input_ref_expression {
        $$ = $1;
    }
    | var_ref_expression {
        $$ = $1;
    }
    ;

over_edge
    : name_label {
        $$ = new OverEdge($1);
    }
    | name_label KW_AS name_label {
        $$ = new OverEdge($1, $3);
    }
    ;

over_edges
    : over_edge {
        auto edge = new OverEdges();
        edge->addEdge($1);
        $$ = edge;
    }
    | over_edges COMMA over_edge {
        $1->addEdge($3);
        $$ = $1;
    }
    ;

over_clause
    : KW_OVER MUL {
        auto edges = new OverEdges();
        auto s = new std::string("*");
        auto edge = new OverEdge(s, nullptr);
        edges->addEdge(edge);
        $$ = new OverClause(edges);
    }
    | KW_OVER MUL KW_REVERSELY {
        auto edges = new OverEdges();
        auto s = new std::string("*");
        auto edge = new OverEdge(s, nullptr);
        edges->addEdge(edge);
        $$ = new OverClause(edges, true);
    }
    | KW_OVER over_edges {
        $$ = new OverClause($2);
    }
    | KW_OVER over_edges KW_REVERSELY {
        $$ = new OverClause($2, true);
    }
    ;

where_clause
    : %empty { $$ = nullptr; }
    | KW_WHERE expression { $$ = new WhereClause($2); }
    ;

when_clause
    : %empty { $$ = nullptr; }
    | KW_WHEN expression { $$ = new WhenClause($2); }
    ;

yield_clause
    : %empty { $$ = nullptr; }
    | KW_YIELD yield_columns { $$ = new YieldClause($2); }
    | KW_YIELD KW_DISTINCT yield_columns { $$ = new YieldClause($3, true); }
    ;

yield_columns
    : yield_column {
        auto fields = new YieldColumns();
        fields->addColumn($1);
        $$ = fields;
    }
    | yield_columns COMMA yield_column {
        $1->addColumn($3);
        $$ = $1;
    }
    ;

yield_column
    : expression {
        $$ = new YieldColumn($1);
    }
    | agg_function L_PAREN expression R_PAREN {
        auto yield = new YieldColumn($3);
        yield->setFunction($1);
        $$ = yield;
    }
    | expression KW_AS name_label {
        $$ = new YieldColumn($1, $3);
    }
    | agg_function L_PAREN expression R_PAREN KW_AS name_label {
        auto yield = new YieldColumn($3, $6);
        yield->setFunction($1);
        $$ = yield;
    }
    | agg_function L_PAREN MUL R_PAREN {
        auto expr = new PrimaryExpression(std::string("*"));
        auto yield = new YieldColumn(expr);
        yield->setFunction($1);
        $$ = yield;
    }
    | agg_function L_PAREN MUL R_PAREN KW_AS name_label {
        auto expr = new PrimaryExpression(std::string("*"));
        auto yield = new YieldColumn(expr, $6);
        yield->setFunction($1);
        $$ = yield;
    }
    ;

group_clause
    : yield_columns { $$ = new GroupClause($1); }
    ;

yield_sentence
    : KW_YIELD yield_columns where_clause {
        auto *s = new YieldSentence($2);
		s->setWhereClause($3);
		$$ = s;
    }
    ;

match_sentence
    : KW_MATCH { $$ = new MatchSentence; }
    ;

find_sentence
    : KW_FIND prop_list KW_FROM name_label where_clause {
        auto sentence = new FindSentence($4, $2);
        sentence->setWhereClause($5);
        $$ = sentence;
    }
    ;

order_factor
    : input_ref_expression {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | input_ref_expression KW_ASC {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | input_ref_expression KW_DESC {
        $$ = new OrderFactor($1, OrderFactor::DESCEND);
    }
    | LABEL {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::ASCEND);
    }
    | LABEL KW_ASC {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::ASCEND);
    }
    | LABEL KW_DESC {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::DESCEND);
    }
    ;

order_factors
    : order_factor {
        auto factors = new OrderFactors();
        factors->addFactor($1);
        $$ = factors;
    }
    | order_factors COMMA order_factor {
        $1->addFactor($3);
        $$ = $1;
    }
    ;

order_by_sentence
    : KW_ORDER KW_BY order_factors {
        $$ = new OrderBySentence($3);
    }
    ;

fetch_vertices_sentence
    : KW_FETCH KW_PROP KW_ON name_label vid_list yield_clause {
        auto fetch = new FetchVerticesSentence($4, $5, $6);
        $$ = fetch;
    }
    | KW_FETCH KW_PROP KW_ON name_label vid_ref_expression yield_clause {
        auto fetch = new FetchVerticesSentence($4, $5, $6);
        $$ = fetch;
    }
    ;

edge_key
    : vid R_ARROW vid AT rank {
        $$ = new EdgeKey($1, $3, $5);
    }
    | vid R_ARROW vid {
        $$ = new EdgeKey($1, $3, 0);
    }
    ;

edge_keys
    : edge_key {
        auto edgeKeys = new EdgeKeys();
        edgeKeys->addEdgeKey($1);
        $$ = edgeKeys;
    }
    | edge_keys COMMA edge_key {
        $1->addEdgeKey($3);
        $$ = $1;
    }
    ;

edge_key_ref:
    input_ref_expression R_ARROW input_ref_expression AT input_ref_expression {
        $$ = new EdgeKeyRef($1, $3, $5);
    }
    |
    var_ref_expression R_ARROW var_ref_expression AT var_ref_expression {
        $$ = new EdgeKeyRef($1, $3, $5, false);
    }
    |
    input_ref_expression R_ARROW input_ref_expression {
        $$ = new EdgeKeyRef($1, $3, nullptr);
    }
    |
    var_ref_expression R_ARROW var_ref_expression {
        $$ = new EdgeKeyRef($1, $3, nullptr, false);
    }
    ;

fetch_edges_sentence
    : KW_FETCH KW_PROP KW_ON name_label edge_keys yield_clause {
        auto fetch = new FetchEdgesSentence($4, $5, $6);
        $$ = fetch;
    }
    | KW_FETCH KW_PROP KW_ON name_label edge_key_ref yield_clause {
        auto fetch = new FetchEdgesSentence($4, $5, $6);
        $$ = fetch;
    }
    ;

fetch_sentence
    : fetch_vertices_sentence { $$ = $1; }
    | fetch_edges_sentence { $$ = $1; }
    ;

find_path_sentence
    : KW_FIND KW_ALL KW_PATH from_clause to_clause over_clause find_path_upto_clause
    /* where_clause */ {
        auto *s = new FindPathSentence(false);
        s->setFrom($4);
        s->setTo($5);
        s->setOver($6);
        s->setStep($7);
        /* s->setWhere($8); */
        $$ = s;
    }
    | KW_FIND KW_SHORTEST KW_PATH from_clause to_clause over_clause find_path_upto_clause
    /* where_clause */ {
        auto *s = new FindPathSentence(true);
        s->setFrom($4);
        s->setTo($5);
        s->setOver($6);
        s->setStep($7);
        /* s->setWhere($8); */
        $$ = s;
    }
    ;

find_path_upto_clause
    : %empty { $$ = new StepClause(5, true); }
    | KW_UPTO INTEGER KW_STEPS { $$ = new StepClause($2, true); }
    ;

to_clause
    : KW_TO vid_list {
        $$ = new ToClause($2);
    }
    | KW_TO vid_ref_expression {
        $$ = new ToClause($2);
    }
    ;

limit_sentence
    : KW_LIMIT INTEGER { $$ = new LimitSentence(0, $2); }
    | KW_LIMIT INTEGER COMMA INTEGER { $$ = new LimitSentence($2, $4); }
    | KW_LIMIT INTEGER KW_OFFSET INTEGER { $$ = new LimitSentence($2, $4); }
    ;

group_by_sentence
    : KW_GROUP KW_BY group_clause yield_clause {
        auto group = new GroupBySentence();
        group->setGroupClause($3);
        group->setYieldClause($4);
        $$ = group;
    }
    ;

use_sentence
    : KW_USE name_label { $$ = new UseSentence($2); }
    ;

opt_create_schema_prop_list
    : %empty {
        $$ = nullptr;
    }
    | create_schema_prop_list {
        $$ = $1;
    }
    ;

create_schema_prop_list
    : create_schema_prop_item {
        $$ = new SchemaPropList();
        $$->addOpt($1);
    }
    | create_schema_prop_list COMMA create_schema_prop_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

create_schema_prop_item
    : KW_TTL_DURATION ASSIGN unary_integer {
        // Less than or equal to 0 means infinity, so less than 0 is equivalent to 0
        if ($3 < 0) {
            $3 = 0;
        }
        $$ = new SchemaPropItem(SchemaPropItem::TTL_DURATION, $3);
    }
    | KW_TTL_COL ASSIGN name_label {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_COL, *$3);
        delete $3;
    }
    ;

create_tag_sentence
    : KW_CREATE KW_TAG name_label L_PAREN R_PAREN opt_create_schema_prop_list {
        if ($6 == nullptr) {
            $6 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($3, new ColumnSpecificationList(), $6);
    }
    | KW_CREATE KW_TAG name_label L_PAREN column_spec_list R_PAREN opt_create_schema_prop_list {
        if ($7 == nullptr) {
            $7 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($3, $5, $7);
    }
    | KW_CREATE KW_TAG name_label L_PAREN column_spec_list COMMA R_PAREN opt_create_schema_prop_list {
        if ($8 == nullptr) {
            $8 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($3, $5, $8);
    }
    ;

alter_tag_sentence
    : KW_ALTER KW_TAG name_label alter_schema_opt_list {
        $$ = new AlterTagSentence($3, $4, new SchemaPropList());
    }
    | KW_ALTER KW_TAG name_label alter_schema_prop_list {
        $$ = new AlterTagSentence($3, new AlterSchemaOptList(), $4);
    }
    | KW_ALTER KW_TAG name_label alter_schema_opt_list alter_schema_prop_list {
        $$ = new AlterTagSentence($3, $4, $5);
    }
    ;

alter_schema_opt_list
    : alter_schema_opt_item {
        $$ = new AlterSchemaOptList();
        $$->addOpt($1);
    }
    | alter_schema_opt_list COMMA alter_schema_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

alter_schema_opt_item
    : KW_ADD L_PAREN column_spec_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::ADD, $3);
    }
    | KW_CHANGE L_PAREN column_spec_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::CHANGE, $3);
    }
    | KW_DROP L_PAREN column_name_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::DROP, $3);
    }
    ;

alter_schema_prop_list
    : alter_schema_prop_item {
        $$ = new SchemaPropList();
        $$->addOpt($1);
    }
    | alter_schema_prop_list COMMA alter_schema_prop_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

alter_schema_prop_item
    : KW_TTL_DURATION ASSIGN unary_integer {
        // Less than or equal to 0 means infinity, so less than 0 is equivalent to 0
        if ($3 < 0) {
            $3 = 0;
        }
        $$ = new SchemaPropItem(SchemaPropItem::TTL_DURATION, $3);
    }
    | KW_TTL_COL ASSIGN name_label {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_COL, *$3);
        delete $3;
    }
    ;

create_edge_sentence
    : KW_CREATE KW_EDGE name_label L_PAREN R_PAREN opt_create_schema_prop_list {
        if ($6 == nullptr) {
            $6 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($3,  new ColumnSpecificationList(), $6);
    }
    | KW_CREATE KW_EDGE name_label L_PAREN column_spec_list R_PAREN opt_create_schema_prop_list {
        if ($7 == nullptr) {
            $7 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($3, $5, $7);
    }
    | KW_CREATE KW_EDGE name_label L_PAREN column_spec_list COMMA R_PAREN opt_create_schema_prop_list {
        if ($8 == nullptr) {
            $8 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($3, $5, $8);
    }
    ;

alter_edge_sentence
    : KW_ALTER KW_EDGE name_label alter_schema_opt_list {
        $$ = new AlterEdgeSentence($3, $4, new SchemaPropList());
    }
    | KW_ALTER KW_EDGE name_label alter_schema_prop_list {
        $$ = new AlterEdgeSentence($3, new AlterSchemaOptList(), $4);
    }
    | KW_ALTER KW_EDGE name_label alter_schema_opt_list alter_schema_prop_list {
        $$ = new AlterEdgeSentence($3, $4, $5);
    }
    ;

column_name_list
    : name_label {
        $$ = new ColumnNameList();
        $$->addColumn($1);
    }
    | column_name_list COMMA name_label {
        $$ = $1;
        $$->addColumn($3);
    }
    ;

column_spec_list
    : column_spec {
        $$ = new ColumnSpecificationList();
        $$->addColumn($1);
    }
    | column_spec_list COMMA column_spec {
        $$ = $1;
        $$->addColumn($3);
    }
    ;

column_spec
    : name_label type_spec { $$ = new ColumnSpecification($2, $1); }
    | name_label type_spec KW_DEFAULT INTEGER {
        $$ = new ColumnSpecification($2, $1);
        $$->setIntValue($4);
    }
    | name_label type_spec KW_DEFAULT BOOL {
        $$ = new ColumnSpecification($2, $1);
        $$->setBoolValue($4);
    }
    | name_label type_spec KW_DEFAULT DOUBLE {
        $$ = new ColumnSpecification($2, $1);
        $$->setDoubleValue($4);
    }
    |  name_label type_spec KW_DEFAULT STRING {
        $$ = new ColumnSpecification($2, $1);
        $$->setStringValue($4);
    }
    ;

describe_tag_sentence
    : KW_DESCRIBE KW_TAG name_label {
        $$ = new DescribeTagSentence($3);
    }
    | KW_DESC KW_TAG name_label {
        $$ = new DescribeTagSentence($3);
    }
    ;

describe_edge_sentence
    : KW_DESCRIBE KW_EDGE name_label {
        $$ = new DescribeEdgeSentence($3);
    }
    | KW_DESC KW_EDGE name_label {
        $$ = new DescribeEdgeSentence($3);
    }
    ;

drop_tag_sentence
    : KW_DROP KW_TAG name_label {
        $$ = new DropTagSentence($3);
    }
    ;

drop_edge_sentence
    : KW_DROP KW_EDGE name_label {
        $$ = new DropEdgeSentence($3);
    }
    ;

traverse_sentence
    : L_PAREN piped_sentence R_PAREN { $$ = $2; }
    | L_PAREN set_sentence R_PAREN { $$ = $2; }
    | go_sentence { $$ = $1; }
    | match_sentence { $$ = $1; }
    | find_sentence { $$ = $1; }
    | group_by_sentence { $$ = $1; }
    | order_by_sentence { $$ = $1; }
    | fetch_sentence { $$ = $1; }
    | find_path_sentence { $$ = $1; }
    | limit_sentence { $$ = $1; }
    | yield_sentence { $$ = $1; }
    ;

set_sentence
    : piped_sentence KW_UNION KW_ALL piped_sentence { $$ = new SetSentence($1, SetSentence::UNION, $4); }
    | piped_sentence KW_UNION piped_sentence {
        auto *s = new SetSentence($1, SetSentence::UNION, $3);
        s->setDistinct();
        $$ = s;
    }
    | piped_sentence KW_UNION KW_DISTINCT piped_sentence {
        auto *s = new SetSentence($1, SetSentence::UNION, $4);
        s->setDistinct();
        $$ = s;
    }
    | piped_sentence KW_INTERSECT piped_sentence { $$ = new SetSentence($1, SetSentence::INTERSECT, $3); }
    | piped_sentence KW_MINUS piped_sentence { $$ = new SetSentence($1, SetSentence::MINUS, $3); }
    | set_sentence KW_UNION KW_ALL piped_sentence { $$ = new SetSentence($1, SetSentence::UNION, $4); }
    | set_sentence KW_UNION piped_sentence {
        auto *s = new SetSentence($1, SetSentence::UNION, $3);
        s->setDistinct();
        $$ = s;
    }
    | set_sentence KW_UNION KW_DISTINCT piped_sentence {
        auto *s = new SetSentence($1, SetSentence::UNION, $4);
        s->setDistinct();
        $$ = s;
    }
    | set_sentence KW_INTERSECT piped_sentence { $$ = new SetSentence($1, SetSentence::INTERSECT, $3); }
    | set_sentence KW_MINUS piped_sentence { $$ = new SetSentence($1, SetSentence::MINUS, $3); }
    ;

piped_sentence
    : traverse_sentence { $$ = $1; }
    | piped_sentence PIPE traverse_sentence { $$ = new PipedSentence($1, $3); }
    ;

assignment_sentence
    : VARIABLE ASSIGN piped_sentence {
        $$ = new AssignmentSentence($1, $3);
    }
    ;

insert_vertex_sentence
    : KW_INSERT KW_VERTEX vertex_tag_list KW_VALUES vertex_row_list {
        $$ = new InsertVertexSentence($3, $5);
    }
    | KW_INSERT KW_VERTEX KW_NO KW_OVERWRITE vertex_tag_list KW_VALUES vertex_row_list {
        $$ = new InsertVertexSentence($5, $7, false /* not overwritable */);
    }
    ;

vertex_tag_list
    : vertex_tag_item {
        $$ = new VertexTagList();
        $$->addTagItem($1);
    }
    | vertex_tag_list COMMA vertex_tag_item {
        $$ = $1;
        $$->addTagItem($3);
    }
    ;

vertex_tag_item
    : name_label L_PAREN R_PAREN{
        $$ = new VertexTagItem($1);
    }
    | name_label L_PAREN prop_list R_PAREN {
        $$ = new VertexTagItem($1, $3);
    }
    ;

prop_list
    : name_label {
        $$ = new PropertyList();
        $$->addProp($1);
    }
    | prop_list COMMA name_label {
        $$ = $1;
        $$->addProp($3);
    }
    | prop_list COMMA {
        $$ = $1;
    }
    ;

vertex_row_list
    : vertex_row_item {
        $$ = new VertexRowList();
        $$->addRow($1);
    }
    | vertex_row_list COMMA vertex_row_item {
        $1->addRow($3);
        $$ = $1;
    }
    ;

vertex_row_item
    : vid COLON L_PAREN R_PAREN {
        $$ = new VertexRowItem($1, new ValueList());
    }
    | vid COLON L_PAREN value_list R_PAREN {
        $$ = new VertexRowItem($1, $4);
    }
    ;

value_list
    : expression {
        $$ = new ValueList();
        $$->addValue($1);
    }
    | value_list COMMA expression {
        $$ = $1;
        $$->addValue($3);
    }
    | value_list COMMA {
        $$ = $1;
    }
    ;

insert_edge_sentence
    : KW_INSERT KW_EDGE name_label L_PAREN R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps(new PropertyList());
        sentence->setRows($7);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE name_label L_PAREN prop_list R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps($5);
        sentence->setRows($8);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE name_label L_PAREN R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps(new PropertyList());
        sentence->setRows($9);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE name_label L_PAREN prop_list R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps($7);
        sentence->setRows($10);
        $$ = sentence;
    }
    ;

edge_row_list
    : edge_row_item {
        $$ = new EdgeRowList();
        $$->addRow($1);
    }
    | edge_row_list COMMA edge_row_item {
        $1->addRow($3);
        $$ = $1;
    }
    ;

edge_row_item
    : vid R_ARROW vid COLON L_PAREN R_PAREN {
        $$ = new EdgeRowItem($1, $3, new ValueList());
    }
    | vid R_ARROW vid COLON L_PAREN value_list R_PAREN {
        $$ = new EdgeRowItem($1, $3, $6);
    }
    | vid R_ARROW vid AT rank COLON L_PAREN R_PAREN {
        $$ = new EdgeRowItem($1, $3, $5, new ValueList());
    }
    | vid R_ARROW vid AT rank COLON L_PAREN value_list R_PAREN {
        $$ = new EdgeRowItem($1, $3, $5, $8);
    }
    ;

rank: unary_integer { $$ = $1; };

update_vertex_sentence
    : KW_UPDATE KW_VERTEX vid KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setVid($3);
        sentence->setUpdateList($5);
        sentence->setWhenClause($6);
        sentence->setYieldClause($7);
        $$ = sentence;
    }
    | KW_UPSERT KW_VERTEX vid KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setInsertable(true);
        sentence->setVid($3);
        sentence->setUpdateList($5);
        sentence->setWhenClause($6);
        sentence->setYieldClause($7);
        $$ = sentence;
    }
    ;

update_list
    : update_item {
        $$ = new UpdateList();
        $$->addItem($1);
    }
    | update_list COMMA update_item {
        $$ = $1;
        $$->addItem($3);
    }
    ;

update_item
    : name_label ASSIGN expression {
        $$ = new UpdateItem($1, $3);
    }
    | alias_ref_expression ASSIGN expression {
        $$ = new UpdateItem($1, $3);
        delete $1;
    }
    ;

update_edge_sentence
    : KW_UPDATE KW_EDGE vid R_ARROW vid KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setEdgeType($7);
        sentence->setUpdateList($9);
        sentence->setWhenClause($10);
        sentence->setYieldClause($11);
        $$ = sentence;
    }
    | KW_UPSERT KW_EDGE vid R_ARROW vid KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setInsertable(true);
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setEdgeType($7);
        sentence->setUpdateList($9);
        sentence->setWhenClause($10);
        sentence->setYieldClause($11);
        $$ = sentence;
    }
    | KW_UPDATE KW_EDGE vid R_ARROW vid AT rank KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setRank($7);
        sentence->setEdgeType($9);
        sentence->setUpdateList($11);
        sentence->setWhenClause($12);
        sentence->setYieldClause($13);
        $$ = sentence;
    }
    | KW_UPSERT KW_EDGE vid R_ARROW vid AT rank KW_OF name_label
      KW_SET update_list when_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setInsertable(true);
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setRank($7);
        sentence->setEdgeType($9);
        sentence->setUpdateList($11);
        sentence->setWhenClause($12);
        sentence->setYieldClause($13);
        $$ = sentence;
    }
    ;

delete_vertex_sentence
    : KW_DELETE KW_VERTEX vid {
        auto sentence = new DeleteVertexSentence($3);
        $$ = sentence;
    }
    ;

download_sentence
    : KW_DOWNLOAD KW_HDFS STRING {
        auto sentence = new DownloadSentence();
        sentence->setUrl($3);
        $$ = sentence;
    }
    ;

edge_list
    : vid R_ARROW vid {
        $$ = new EdgeList();
        $$->addEdge($1, $3);
    }
    | edge_list COMMA vid R_ARROW vid {
        $$ = $1;
        $$->addEdge($3, $5);
    }
    ;

delete_edge_sentence
    : KW_DELETE KW_EDGE edge_list where_clause {
        auto sentence = new DeleteEdgeSentence($3);
        sentence->setWhereClause($4);
        $$ = sentence;
    }
    ;

ingest_sentence
    : KW_INGEST {
        auto sentence = new IngestSentence();
        $$ = sentence;
    }
    ;

show_sentence
    : KW_SHOW KW_HOSTS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowHosts);
    }
    | KW_SHOW KW_SPACES {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowSpaces);
    }
    | KW_SHOW KW_PARTS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowParts);
    }
    | KW_SHOW KW_TAGS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowTags);
    }
    | KW_SHOW KW_EDGES {
         $$ = new ShowSentence(ShowSentence::ShowType::kShowEdges);
    }
    | KW_SHOW KW_USERS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowUsers);
    }
    | KW_SHOW KW_USER name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowUser, $3);
    }
    | KW_SHOW KW_ROLES KW_IN name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowRoles, $4);
    }
    | KW_SHOW KW_CONFIGS show_config_item {
        $$ = new ConfigSentence(ConfigSentence::SubType::kShow, $3);
    }
    | KW_SHOW KW_CREATE KW_SPACE name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCreateSpace, $4);
    }
    | KW_SHOW KW_CREATE KW_TAG name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCreateTag, $4);
    }
    | KW_SHOW KW_CREATE KW_EDGE name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowCreateEdge, $4);
    }
    ;

config_module_enum
    : KW_GRAPH      { $$ = ConfigModule::GRAPH; }
    | KW_META       { $$ = ConfigModule::META; }
    | KW_STORAGE    { $$ = ConfigModule::STORAGE; }
    ;

get_config_item
    : config_module_enum COLON name_label {
        $$ = new ConfigRowItem($1, $3);
    }
    | name_label {
        $$ = new ConfigRowItem(ConfigModule::ALL, $1);
    }
    ;

set_config_item
    : config_module_enum COLON name_label ASSIGN expression {
        $$ = new ConfigRowItem($1, $3, $5);
    }
    | name_label ASSIGN expression {
        $$ = new ConfigRowItem(ConfigModule::ALL, $1, $3);
    }
    | config_module_enum COLON name_label ASSIGN L_BRACE update_list R_BRACE {
        $$ = new ConfigRowItem($1, $3, $6);
    }
    | name_label ASSIGN L_BRACE update_list R_BRACE {
        $$ = new ConfigRowItem(ConfigModule::ALL, $1, $4);
    }
    ;

show_config_item
    : %empty {
        $$ = nullptr;
    }
    | config_module_enum {
        $$ = new ConfigRowItem($1);
    }
    ;

create_space_sentence
    : KW_CREATE KW_SPACE name_label {
        auto sentence = new CreateSpaceSentence($3);
        $$ = sentence;
    }
    | KW_CREATE KW_SPACE name_label L_PAREN space_opt_list R_PAREN {
        auto sentence = new CreateSpaceSentence($3);
        sentence->setOpts($5);
        $$ = sentence;
    }
    ;

describe_space_sentence
    : KW_DESCRIBE KW_SPACE name_label {
        $$ = new DescribeSpaceSentence($3);
    }
    | KW_DESC KW_SPACE name_label {
        $$ = new DescribeSpaceSentence($3);
    }
    ;

space_opt_list
    : space_opt_item {
        $$ = new SpaceOptList();
        $$->addOpt($1);
    }
    | space_opt_list COMMA space_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    | space_opt_list COMMA {
        $$ = $1;
    }
    ;

space_opt_item
    : KW_PARTITION_NUM ASSIGN INTEGER {
        $$ = new SpaceOptItem(SpaceOptItem::PARTITION_NUM, $3);
    }
    | KW_REPLICA_FACTOR ASSIGN INTEGER {
        $$ = new SpaceOptItem(SpaceOptItem::REPLICA_FACTOR, $3);
    }
    // TODO(YT) Create Spaces for different engines
    // KW_ENGINE_TYPE ASSIGN name_label
    ;

drop_space_sentence
    : KW_DROP KW_SPACE name_label {
        $$ = new DropSpaceSentence($3);
    }
    ;

//  User manager sentences.

with_user_opt_item
    : KW_FIRSTNAME STRING {
        $$ = new WithUserOptItem(WithUserOptItem::FIRST, $2);
    }
    | KW_LASTNAME STRING {
        $$ = new WithUserOptItem(WithUserOptItem::LAST, $2);
    }
    | KW_EMAIL STRING {
        $$ = new WithUserOptItem(WithUserOptItem::EMAIL, $2);
    }
    | KW_PHONE STRING {
        $$ = new WithUserOptItem(WithUserOptItem::PHONE, $2);
    }
    ;

with_user_opt_list
    : with_user_opt_item {
        $$ = new WithUserOptList();
        $$->addOpt($1);
    }
    | with_user_opt_list COMMA with_user_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

create_user_sentence
    : KW_CREATE KW_USER name_label KW_WITH KW_PASSWORD STRING {
        $$ = new CreateUserSentence($3, $6);
    }
    | KW_CREATE KW_USER KW_IF KW_NOT KW_EXISTS name_label KW_WITH KW_PASSWORD STRING {
        auto sentence = new CreateUserSentence($6, $9);
        sentence->setMissingOk(true);
        $$ = sentence;
    }
    | KW_CREATE KW_USER name_label KW_WITH KW_PASSWORD STRING COMMA with_user_opt_list {
        auto sentence = new CreateUserSentence($3, $6);
        sentence->setOpts($8);
        $$ = sentence;
    }
    | KW_CREATE KW_USER KW_IF KW_NOT KW_EXISTS name_label KW_WITH KW_PASSWORD STRING COMMA with_user_opt_list {
        auto sentence = new CreateUserSentence($6, $9);
        sentence->setMissingOk(true);
        sentence->setOpts($11);
        $$ = sentence;
    }

alter_user_sentence
    : KW_ALTER KW_USER name_label KW_WITH with_user_opt_list {
        auto sentence = new AlterUserSentence($3);
        sentence->setOpts($5);
        $$ = sentence;
    }
    ;

drop_user_sentence
    : KW_DROP KW_USER name_label {
        $$ = new DropUserSentence($3);
    }
    | KW_DROP KW_USER KW_IF KW_EXISTS name_label {
        auto sentence = new DropUserSentence($5);
        sentence->setMissingOk(true);
        $$ = sentence;
    }
    ;

change_password_sentence
    : KW_CHANGE KW_PASSWORD name_label KW_TO STRING {
        auto sentence = new ChangePasswordSentence($3, $5);
        $$ = sentence;
    }
    | KW_CHANGE KW_PASSWORD name_label KW_FROM STRING KW_TO STRING {
        auto sentence = new ChangePasswordSentence($3, $5, $7);
        $$ = sentence;
    }
    ;

role_type_clause
    : KW_GOD { $$ = new RoleTypeClause(RoleTypeClause::GOD); }
    | KW_ADMIN { $$ = new RoleTypeClause(RoleTypeClause::ADMIN); }
    | KW_USER { $$ = new RoleTypeClause(RoleTypeClause::USER); }
    | KW_GUEST { $$ = new RoleTypeClause(RoleTypeClause::GUEST); }
    ;

acl_item_clause
    : KW_ROLE role_type_clause KW_ON name_label {
        auto sentence = new AclItemClause(true, $4);
        sentence->setRoleTypeClause($2);
        $$ = sentence;
    }
    | role_type_clause KW_ON name_label {
        auto sentence = new AclItemClause(false, $3);
        sentence->setRoleTypeClause($1);
        $$ = sentence;
    }
    ;

grant_sentence
    : KW_GRANT acl_item_clause KW_TO name_label {
        auto sentence = new GrantSentence($4);
        sentence->setAclItemClause($2);
        $$ = sentence;
    }
    ;

revoke_sentence
    : KW_REVOKE acl_item_clause KW_FROM name_label {
        auto sentence = new RevokeSentence($4);
        sentence->setAclItemClause($2);
        $$ = sentence;
    }
    ;

get_config_sentence
    : KW_GET KW_CONFIGS get_config_item {
        $$ = new ConfigSentence(ConfigSentence::SubType::kGet, $3);
    }
    ;

set_config_sentence
    : KW_UPDATE KW_CONFIGS set_config_item  {
        $$ = new ConfigSentence(ConfigSentence::SubType::kSet, $3);
    }
    ;

host_list
    : host_item {
        $$ = new HostList();
        $$->addHost($1);
    }
    | host_list COMMA host_item {
        $$ = $1;
        $$->addHost($3);
    }
    | host_list COMMA {
        $$ = $1;
    }
    ;

host_item
    : IPV4 COLON port {
        $$ = new nebula::HostAddr();
        $$->first = $1;
        $$->second = $3;
    }

port : INTEGER { $$ = $1; }

balance_sentence
    : KW_BALANCE KW_LEADER {
        $$ = new BalanceSentence(BalanceSentence::SubType::kLeader);
    }
    | KW_BALANCE KW_DATA {
        $$ = new BalanceSentence(BalanceSentence::SubType::kData);
    }
    | KW_BALANCE KW_DATA INTEGER {
        $$ = new BalanceSentence($3);
    }
    | KW_BALANCE KW_DATA KW_STOP {
        $$ = new BalanceSentence(BalanceSentence::SubType::kDataStop);
    }
    | KW_BALANCE KW_DATA KW_REMOVE host_list {
        $$ = new BalanceSentence(BalanceSentence::SubType::kData, $4);
    }
    ;

mutate_sentence
    : insert_vertex_sentence { $$ = $1; }
    | insert_edge_sentence { $$ = $1; }
    | update_vertex_sentence { $$ = $1; }
    | update_edge_sentence { $$ = $1; }
    | delete_vertex_sentence { $$ = $1; }
    | delete_edge_sentence { $$ = $1; }
    | download_sentence { $$ = $1; }
    | ingest_sentence { $$ = $1; }
    ;

maintain_sentence
    : create_tag_sentence { $$ = $1; }
    | create_edge_sentence { $$ = $1; }
    | alter_tag_sentence { $$ = $1; }
    | alter_edge_sentence { $$ = $1; }
    | describe_tag_sentence { $$ = $1; }
    | describe_edge_sentence { $$ = $1; }
    | drop_tag_sentence { $$ = $1; }
    | drop_edge_sentence { $$ = $1; }
    | show_sentence { $$ = $1; }
    | create_space_sentence { $$ = $1; }
    | describe_space_sentence { $$ = $1; }
    | drop_space_sentence { $$ = $1; }
    ;
    | create_user_sentence { $$ = $1; }
    | alter_user_sentence { $$ = $1; }
    | drop_user_sentence { $$ = $1; }
    | change_password_sentence { $$ = $1; }
    | grant_sentence { $$ = $1; }
    | revoke_sentence { $$ = $1; }
    | get_config_sentence { $$ = $1; }
    | set_config_sentence { $$ = $1; }
    | balance_sentence { $$ = $1; }
    ;

return_sentence
    : KW_RETURN VARIABLE KW_IF VARIABLE KW_IS KW_NOT KW_NULL {
        $$ = new ReturnSentence($2, $4);
    }

process_control_sentence
    : return_sentence { $$ = $1; }
    ;

sentence
    : maintain_sentence { $$ = $1; }
    | use_sentence { $$ = $1; }
    | set_sentence { $$ = $1; }
    | piped_sentence { $$ = $1; }
    | assignment_sentence { $$ = $1; }
    | mutate_sentence { $$ = $1; }
    | process_control_sentence { $$ = $1; }
    ;

sentences
    : sentence {
        $$ = new SequentialSentences($1);
        *sentences = $$;
    }
    | sentences SEMICOLON sentence {
        if ($1 == nullptr) {
            $$ = new SequentialSentences($3);
            *sentences = $$;
        } else {
            $$ = $1;
            $1->addSentence($3);
        }
    }
    | sentences SEMICOLON {
        $$ = $1;
    }
    | %empty {
        $$ = nullptr;
    }
    ;


%%

void nebula::GraphParser::error(const nebula::GraphParser::location_type& loc,
                                const std::string &msg) {
    std::ostringstream os;
    os << msg << " at " << loc;
    errmsg = os.str();
}


#include "GraphScanner.h"
static int yylex(nebula::GraphParser::semantic_type* yylval,
                 nebula::GraphParser::location_type *yylloc,
                 nebula::GraphScanner& scanner) {
    return scanner.yylex(yylval, yylloc);
}

