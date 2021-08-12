/*
 * Copyright 2021  Sutou Kouhei <kou@clear-code.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <arrow-glib/arrow-glib.hpp>

#include <arrow/c/bridge.h>

#include <rbgobject.h>

#include <ruby-duckdb.h>

#include "arrow-duckdb-registration.hpp"

extern "C" void Init_arrow_duckdb(void);

namespace {
#  if !GARROW_VERSION_CHECK(6, 0, 0)
  GArrowSchema *
  garrow_schema_import(gpointer c_abi_schema,
                       GError **error)
  {
    auto arrow_schema =
      *arrow::ImportSchema(static_cast<ArrowSchema *>(c_abi_schema));
    return garrow_schema_new_raw(&arrow_schema);
  }

  GArrowRecordBatch *
  garrow_record_batch_import(gpointer c_abi_array,
                             GArrowSchema *schema,
                             GError **error)
  {
    auto arrow_schema = garrow_schema_get_raw(schema);
    auto arrow_record_batch =
      *arrow::ImportRecordBatch(static_cast<ArrowArray *>(c_abi_array),
                                arrow_schema);
    return garrow_record_batch_new_raw(&arrow_record_batch);
  }
#  endif

  VALUE cArrowTable;
  VALUE cArrowDuckDBResult;

  struct Result {
    duckdb_arrow arrow;
    char *error_message;
    GArrowSchema *gschema;
  };

  void
  result_free(void *data)
  {
    Result *result = static_cast<Result *>(data);
    if (result->gschema) {
      g_object_unref(result->gschema);
    }
    free(result->error_message);
    duckdb_destroy_arrow(&(result->arrow));
  }

  static const rb_data_type_t result_type = {
    "ArrowDuckDB::Result",
    {
      nullptr,
      result_free,
    },
    nullptr,
    nullptr,
    RUBY_TYPED_FREE_IMMEDIATELY,
  };

  VALUE
  result_alloc_func(VALUE klass)
  {
    Result *result;
    auto rb_result = TypedData_Make_Struct(klass,
                                           Result,
                                           &result_type,
                                           result);
    result->arrow = nullptr;
    result->error_message = nullptr;
    result->gschema = nullptr;
    return rb_result;
  }

  void
  result_ensure_gschema(Result *result)
  {
    ArrowSchema c_abi_schema;
    duckdb_arrow_schema schema = &c_abi_schema;
    auto state = duckdb_query_arrow_schema(result->arrow, &schema);
    if (state == DuckDBError) {
      free(result->error_message);
      result->error_message =
        const_cast<char *>(duckdb_query_arrow_error(result->arrow));
      rb_raise(eDuckDBError,
               "Failed to fetch Apache Arrow schema: %s",
               result->error_message);
    }

    GError *gerror = nullptr;
    result->gschema = garrow_schema_import(&c_abi_schema, &gerror);
    if (gerror) {
      RG_RAISE_ERROR(gerror);
    }
  }

  VALUE
  result_fetch(VALUE self)
  {
    Result *result;
    TypedData_Get_Struct(self, Result, &result_type, result);

    result_ensure_gschema(result);

    ArrowArray c_abi_array = {};
    duckdb_arrow_array array = &c_abi_array;
    auto state = duckdb_query_arrow_array(result->arrow, &array);
    if (state == DuckDBError) {
      free(result->error_message);
      result->error_message =
        const_cast<char *>(duckdb_query_arrow_error(result->arrow));
      rb_raise(eDuckDBError,
               "Failed to fetch Apache Arrow array: %s",
               result->error_message);
    }
    if (!c_abi_array.release) {
      return Qnil;
    }

    GError *gerror = nullptr;
    auto grecord_batch = garrow_record_batch_import(&c_abi_array,
                                                    result->gschema,
                                                    &gerror);
    if (gerror) {
      RG_RAISE_ERROR(gerror);
      return Qnil;
    }
    return GOBJ2RVAL_UNREF(grecord_batch);
  }

  VALUE
  query_sql_arrow(VALUE self, VALUE sql)
  {
    rubyDuckDBConnection *ctx;
    Data_Get_Struct(self, rubyDuckDBConnection, ctx);

    if (!(ctx->con)) {
      rb_raise(eDuckDBError, "Database connection closed");
    }

    ID id_new;
    CONST_ID(id_new, "new");
    auto result = rb_funcall(cArrowDuckDBResult, id_new, 0);
    Result *arrow_duckdb_result;
    TypedData_Get_Struct(result, Result, &result_type, arrow_duckdb_result);
    auto state = duckdb_query_arrow(ctx->con,
                                    StringValueCStr(sql),
                                    &(arrow_duckdb_result->arrow));
    if (state == DuckDBError) {
      if (arrow_duckdb_result->arrow) {
        arrow_duckdb_result->error_message =
          const_cast<char *>(
            duckdb_query_arrow_error(arrow_duckdb_result->arrow));
        rb_raise(eDuckDBError,
                 "Failed to execute query: %s",
                 arrow_duckdb_result->error_message);
      } else {
        rb_raise(eDuckDBError, "Failed to execute query");
      }
    }

    return result;
  }

  VALUE
  query_unregister_arrow(VALUE self, VALUE name)
  {
    rubyDuckDBConnection *ctx;
    Data_Get_Struct(self, rubyDuckDBConnection, ctx);

    if (!(ctx->con)) {
      rb_raise(eDuckDBError, "Database connection closed");
    }

    arrow_duckdb::connection_unregister(ctx->con, name);

    auto arrow_tables = rb_iv_get(self, "@arrow_tables");
    if (NIL_P(arrow_tables)) {
      arrow_tables = rb_hash_new();
      rb_iv_set(self, "@arrow_tables", arrow_tables);
    }
    rb_hash_delete(arrow_tables, name);

    return self;
  }

  VALUE
  query_register_arrow_body(VALUE)
  {
    return rb_yield_values(0);
  }

  struct QueryRegisterArrowData {
    VALUE self;
    VALUE name;
  };

  VALUE
  query_register_arrow_ensure(VALUE user_data)
  {
    auto data = reinterpret_cast<QueryRegisterArrowData *>(user_data);
    return query_unregister_arrow(data->self, data->name);
  }

  VALUE
  query_register_arrow(VALUE self, VALUE name, VALUE arrow_table)
  {
    rubyDuckDBConnection *ctx;
    Data_Get_Struct(self, rubyDuckDBConnection, ctx);

    if (!(ctx->con)) {
      rb_raise(eDuckDBError, "Database connection closed");
    }

    if (!RVAL2CBOOL(rb_obj_is_kind_of(arrow_table, cArrowTable))) {
      rb_raise(rb_eArgError, "must be Arrow::Table: %" PRIsVALUE, arrow_table);
    }

    arrow_duckdb::connection_register(ctx->con, name, arrow_table);

    auto arrow_tables = rb_iv_get(self, "@arrow_tables");
    if (NIL_P(arrow_tables)) {
      arrow_tables = rb_hash_new();
      rb_iv_set(self, "@arrow_tables", arrow_tables);
    }
    rb_hash_aset(arrow_tables, name, arrow_table);

    if (rb_block_given_p()) {
      QueryRegisterArrowData data;
      data.self = self;
      data.name = name;
      return rb_ensure(query_register_arrow_body,
                       Qnil,
                       query_register_arrow_ensure,
                       reinterpret_cast<VALUE>(&data));
    } else {
      return self;
    }
  }

  void init()
  {
    cArrowTable = rb_const_get(rb_const_get(rb_cObject, rb_intern("Arrow")),
                               rb_intern("Table"));

    VALUE mArrowDuckDB;
    mArrowDuckDB = rb_define_module("ArrowDuckDB");
    cArrowDuckDBResult = rb_define_class_under(mArrowDuckDB,
                                               "Result",
                                               rb_cObject);
    rb_define_alloc_func(cArrowDuckDBResult, result_alloc_func);
    rb_define_method(cArrowDuckDBResult, "fetch", result_fetch, 0);

    rb_define_method(cDuckDBConnection, "query_sql_arrow", query_sql_arrow, 1);
    rb_define_method(cDuckDBConnection,
                     "register_arrow",
                     query_register_arrow,
                     2);
    rb_define_method(cDuckDBConnection,
                     "unregister_arrow",
                     query_unregister_arrow,
                     1);
  }
}

extern "C" void
Init_arrow_duckdb(void)
{
  init();
}
