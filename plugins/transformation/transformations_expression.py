import re
from datetime import datetime
from typing import Any, Dict

from exceptions.custom_exception import MorphusAirflowException


class TransformationExpression:
    """
    Explicit-function expression evaluator (SAFE, NO eval).
    """

    def __init__(self, header_mapping: Dict[str, int]):
        self.header_mapping = header_mapping

    # --------------------------------------------------
    def _get_col_value(self, col: str, row: list[Any]) -> Any:
        try:
            idx = self.header_mapping.get(col.lower())
            if idx is None or idx >= len(row):
                return ""
            return row[idx]

        except Exception as e:
            raise MorphusAirflowException(
                message=f"Failed to resolve column value '{col}'",
                error_source="TransformationExpression._get_col_value",
                original_exception=e
            ) from e

    # --------------------------------------------------
    def evaluate(self, expression: str, row: list[Any]) -> Any:
        try:
            expr = expression.strip()

            m = re.match(r"literal\((.*)\)$", expr, re.IGNORECASE)
            if m:
                return self.evaluate(m.group(1).strip(), row)

            m = re.match(r"concat\((.*)\)$", expr, re.IGNORECASE)
            if m:
                parts = self._split_args(m.group(1))
                return "".join(str(self.evaluate(p, row)) for p in parts)

            m = re.match(r"filter\((.*)\)$", expr, re.IGNORECASE)
            if m:
                return self.evaluate(m.group(1).strip(), row)

            m = re.match(r"if\((.*?),(.*?),(.*)\)$", expr, re.IGNORECASE)
            if m:
                condition = m.group(1).strip()
                true_val = m.group(2).strip()
                false_val = m.group(3).strip()
                return (
                    self.evaluate(true_val, row)
                    if self._eval_condition(condition, row)
                    else self.evaluate(false_val, row)
                )

            m = re.match(r"formula\((.*)\)$", expr, re.IGNORECASE)
            if m:
                return self._eval_arithmetic(m.group(1).strip(), row)

            if expr.lower().startswith("int(") and expr.endswith(")"):
                return int(self.evaluate(expr[4:-1], row))

            if expr.lower().startswith("float(") and expr.endswith(")"):
                return float(self.evaluate(expr[6:-1], row))

            if re.fullmatch(r"\d+", expr):
                return int(expr)

            if re.fullmatch(r"\d+\.\d+", expr):
                return float(expr)

            if (expr.startswith("'") and expr.endswith("'")) or (
                expr.startswith('"') and expr.endswith('"')
            ):
                return expr[1:-1]

            if expr.upper() == "CURRENT_DATE":
                return datetime.utcnow().date().isoformat()

            if expr.upper() == "NOW()":
                return datetime.utcnow().isoformat()

            if expr.lower() in self.header_mapping:
                return self._get_col_value(expr, row)

            raise MorphusAirflowException(
                message=f"Unsupported expression '{expression}'",
                error_source="TransformationExpression.evaluate"
            )

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message=f"Error evaluating expression '{expression}'",
                error_source="TransformationExpression.evaluate",
                original_exception=e
            ) from e

    # --------------------------------------------------
    def _eval_arithmetic(self, expr: str, row: list[Any]) -> Any:
        try:
            for op in ["*", "+", "-", "/"]:
                if op in expr:
                    left, right = expr.split(op, 1)
                    l = float(self.evaluate(left.strip(), row))
                    r = float(self.evaluate(right.strip(), row))
                    return {
                        "*": l * r,
                        "+": l + r,
                        "-": l - r,
                        "/": l / r,
                    }[op]

            raise MorphusAirflowException(
                message=f"Unsupported arithmetic formula '{expr}'",
                error_source="TransformationExpression._eval_arithmetic"
            )

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message=f"Error evaluating arithmetic formula '{expr}'",
                error_source="TransformationExpression._eval_arithmetic",
                original_exception=e
            ) from e

    # --------------------------------------------------
    def _split_args(self, args: str) -> list[str]:
        try:
            parts, current, in_quotes = [], "", False
            for c in args:
                if c in "'\"" and not in_quotes:
                    in_quotes = True
                elif c in "'\"" and in_quotes:
                    in_quotes = False

                if c == "," and not in_quotes:
                    parts.append(current.strip())
                    current = ""
                else:
                    current += c

            if current:
                parts.append(current.strip())

            return parts

        except Exception as e:
            raise MorphusAirflowException(
                message="Failed while splitting expression arguments",
                error_source="TransformationExpression._split_args",
                original_exception=e
            ) from e

    # --------------------------------------------------
    def _eval_condition(self, cond: str, row: list[Any]) -> bool:
        try:
            for op in [">=", "<=", ">", "<", "==", "!="]:
                if op in cond:
                    left, right = cond.split(op, 1)
                    l = float(self.evaluate(left.strip(), row))
                    r = float(self.evaluate(right.strip(), row))
                    return {
                        ">": l > r,
                        "<": l < r,
                        ">=": l >= r,
                        "<=": l <= r,
                        "==": l == r,
                        "!=": l != r,
                    }[op]

            raise MorphusAirflowException(
                message=f"Unsupported condition '{cond}'",
                error_source="TransformationExpression._eval_condition"
            )

        except MorphusAirflowException:
            raise

        except Exception as e:
            raise MorphusAirflowException(
                message=f"Error evaluating condition '{cond}'",
                error_source="TransformationExpression._eval_condition",
                original_exception=e
            ) from e
