CREATE IF NOT EXISTS TABLE public.audit_log (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    transaction_id bigint DEFAULT txid_current() NOT NULL,
    query text NOT NULL,
    query_type text NOT NULL,
    activity text,
    old_data jsonb,
    new_data jsonb,
    user_id text,
    user_type text DEFAULT 'system'::text NOT NULL,
    "timestamp" timestamp with time zone DEFAULT now() NOT NULL,
    db_user text DEFAULT "current_user"() NOT NULL
);


ALTER TABLE ONLY public.audit_log
    ADD CONSTRAINT audit_log_pkey PRIMARY KEY (id);

CREATE INDEX audit_log_entity_type_entity_id_idx ON public.audit_log USING btree (entity_type, entity_id);

CREATE INDEX audit_log_timestamp_id_idx ON public.audit_log USING btree (date_trunc('milliseconds'::text, timezone('UTC'::text, "timestamp")), id);

CREATE FUNCTION public.general_audit_log_insert(_entity_type text, _entity_id text, _query text, _query_type text, _activity text, _old_json jsonb, _new_json jsonb, _user_id text, _user_type text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
BEGIN
  EXECUTE 'INSERT INTO public.audit_log(
	entity_type, entity_id, query, query_type, activity, old_data, new_data, user_id, user_type
  ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);'
  USING
    _entity_type,
    _entity_id,
    _query,
    _query_type,
    _activity,
    _old_json,
    _new_json,
    _user_id,
    _user_type;

END
$_$;

CREATE FUNCTION public.audit_log_object_diff(in_old jsonb, in_new jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
  _key text;
  _value jsonb;
  _old jsonb;
  _new jsonb;
  _same jsonb;
BEGIN
  _old := in_old;
  _new := in_new;

  FOR _key, _value IN SELECT * FROM jsonb_each(_old) LOOP
    IF (_new -> _key) = _value THEN
      _old := _old - _key;
      _new := _new - _key;
      IF _same IS NULL THEN
        _same := jsonb_build_object(_key, _value);
      ELSE
        _same := _same || jsonb_build_object(_key, _value);
      END IF;
    END IF;
  END LOOP;

  RETURN json_build_object('old', _old, 'new', _new, 'same', _same);
END;
$$;


CREATE FUNCTION public.general_audit_log_entries_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  _entity_type text;
  _entity_id text;
  _query text;
  _query_type text;
  _activity text;
  _old_json jsonb;
  _new_json jsonb;
  _user_id text;
  _user_type text;
  _new_to_json jsonb;
  _old_to_json jsonb;
BEGIN
  IF TG_NARGS <> 2 THEN RAISE 'MISSING PARAMETERS IN AUDIT LOG TRIGGER. (primary key text, _black_list ARRAY[string])'; END IF;

  _entity_id := TG_ARGV[0];
  _entity_type := TG_TABLE_NAME::text; -- TABLE NAME
  _query := current_query()::text; -- QUERY CALLED
  _query_type := TG_OP::text; -- QUERY OPERATION INSERT OR UPDATE
  _user_id := current_setting('audit.userId', true)::text; -- USER ID
  _user_type := current_setting('audit.userType',true)::text; -- USER TYPE
  _activity := current_setting('audit.activity', true)::text; -- ACTIVITY

  IF _user_type IS NULL THEN _user_type := 'system'; END IF;

  IF TG_OP = 'INSERT' THEN
    _new_to_json := row_to_json(NEW.*)::jsonb;
    _old_json := NULL;

    SELECT result.* INTO _new_json FROM jsonb_remove_columns(_new_to_json, TG_ARGV[1]::text[]) as result;

    _entity_id := _new_to_json ->_entity_id; -- GET PRIMARY KEY VALUE
  ELSEIF TG_OP = 'UPDATE' THEN
    _new_to_json := row_to_json(NEW.*)::jsonb;
    _old_to_json := row_to_json(OLD.*)::jsonb;

    SELECT result.* INTO _new_json FROM jsonb_remove_columns(_new_to_json, TG_ARGV[1]::text[]) as result;
    SELECT result.* INTO _old_json FROM jsonb_remove_columns(_old_to_json, TG_ARGV[1]::text[]) as result;

    _entity_id := _new_to_json ->_entity_id; -- GET PRIMARY KEY VALUE
  ELSEIF TG_OP = 'DELETE' THEN
	  _new_json := NULL;
    _old_to_json := row_to_json(OLD.*)::jsonb;

    SELECT result.* INTO _old_json FROM jsonb_remove_columns(_old_to_json, TG_ARGV[1]::text[]) as result;

    _entity_id := _old_to_json ->_entity_id; -- GET PRIMARY KEY VALUE
  END IF;

  -- INSERT NEW ITEM TO AUDIT LOG
  EXECUTE general_audit_log_insert(
    _entity_type,
    _entity_id,
    _query,
    _query_type,
    _activity,
    _old_json,
    _new_json,
    _user_id,
    _user_type
  );
RETURN NULL;
END;
$$;

--
-- Name: jsonb_remove_columns(jsonb, text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.jsonb_remove_columns(json_input jsonb, column_name_array text[]) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
  _item text;
BEGIN

  FOREACH _item IN ARRAY column_name_array
  LOOP
  	IF (json_input->_item) IS NOT NULL THEN
    	json_input := json_input - _item;
	END IF;
  END LOOP;

  RETURN json_input;
END;
$$;


CREATE TRIGGER <table_name>_audit_log AFTER INSERT OR DELETE OR UPDATE ON public.<table_name> FOR EACH ROW EXECUTE PROCEDURE public.general_audit_log_entries_trigger('id', '{}');
