class Condition:
    def __init__(self, condition_type, *conditions):
        """
        Initialize a condition.
        :param condition_type: "and", "or", or "leaf" (comparison condition)
        :param conditions: Subconditions for "and"/"or", or a single key-value tuple for "leaf".
        """
        self.condition_type = condition_type
        self.conditions = conditions

    def evaluate(self, obj):
        """
        Evaluate the condition against the given object.
        :param obj: A dictionary to evaluate against.
        :return: True or False
        """
        if self.condition_type == "and":
            return all(condition.evaluate(obj) for condition in self.conditions)
        elif self.condition_type == "or":
            return any(condition.evaluate(obj) for condition in self.conditions)
        elif self.condition_type == "leaf":
            key, value = self.conditions[0]
            if value == "*":  # Key must simply exist
                return key in obj
            elif value is None:  # Legacy single key existence check
                return key in obj
            else:  # Standard key-value match
                return obj.get(key) == value
        else:
            raise ValueError("Invalid condition type")

    def __repr__(self):
        if self.condition_type == "leaf":
            key, value = self.conditions[0]
            if value == "*":
                return f"{key}:*"
            elif value is not None:
                return f"{key}={value}"
            else:
                return f"{key}"
        else:
            op = " AND " if self.condition_type == "and" else " OR "
            return f"({op.join(map(str, self.conditions))})"