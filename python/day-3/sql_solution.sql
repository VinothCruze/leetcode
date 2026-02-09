CREATE OR REPLACE FUNCTION NthHighestSalary(N INT)
RETURNS TABLE (salary INT)
LANGUAGE sql
AS $$
  SELECT (
      SELECT DISTINCT salary
      FROM Employee
      ORDER BY salary DESC
      OFFSET N - 1
      LIMIT 1
  );
$$;
