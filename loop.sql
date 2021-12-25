DO $$
BEGIN
    FOR counter IN 1..10 LOOP
        INSERT INTO purchases (purchase_id, temperature, price)
            VALUES (counter, 15, 20);
    END LOOP;
END;
$$