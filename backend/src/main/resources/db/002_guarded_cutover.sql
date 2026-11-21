CREATE FUNCTION try_cutover(source_index text, target_index text, confirmed_seq bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    UPDATE index_control SET active = target_index
    WHERE singleton AND active = source_index AND source_index <> target_index
      AND committed_seq = confirmed_seq;
    RETURN FOUND;
END;
$$;
