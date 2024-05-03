import unittest
from ezstorage.tokenizer import LambdaTokenizer

class Car:
    id = 0
    number = 0


class TestLambdaCleanTokenizer(unittest.TestCase):
    """Test the lambda expression cleaner"""

    def test_lambda_tokenizer_with_variable(self):
        x = lambda: Car.id > 0
        cleaned = LambdaTokenizer.get_expression(x)
        self.assertEqual(cleaned, "Car.id > 0")

    def test_lambda_tokenizer_with_long_variable(self):
        x = lambda: Car.id < 0 and Car.id > 0 and Car.id == 0 or Car.id != 0
        cleaned = LambdaTokenizer.get_expression(x)
        self.assertEqual(cleaned, "Car.id < 0 and Car.id > 0 and Car.id == 0 or Car.id != 0")

    def test_lambda_tokenizer_with_long_variable_and_parentheses(self):
        x = lambda: (Car.id < 0 and Car.id > 0) and Car.id == 0 or Car.id != 0
        cleaned = LambdaTokenizer.get_expression(x)
        self.assertEqual(cleaned, "(Car.id < 0 and Car.id > 0) and Car.id == 0 or Car.id != 0")

    def test_lambda_tokenizer_with_long_variable_and_parentheses_and_nested_parentheses(self):
        x = lambda: (Car.id < 0 and Car.id > 0) and (Car.id == 0 or Car.id != 0)
        cleaned = LambdaTokenizer.get_expression(x)
        self.assertEqual(cleaned, "(Car.id < 0 and Car.id > 0) and (Car.id == 0 or Car.id != 0)")

    def test_lambda_tokenizer_with_long_variable_and_parentheses_and_nested_parentheses_and_nested_parentheses(self):
        x = lambda: (Car.id < 0 and Car.id > 0) and (Car.id == 0 or (Car.id != 0 and Car.id >= 0))
        cleaned = LambdaTokenizer.get_expression(x)
        self.assertEqual(cleaned, "(Car.id < 0 and Car.id > 0) and (Car.id == 0 or (Car.id != 0 and Car.id >= 0))")

    def test_lambda_tokenizer_with_variable(self):
        var = 0
        x = lambda: Car.id > var
        cleaned = LambdaTokenizer.get_expression(x)
        self.assertEqual(cleaned, "Car.id > var")

    def test_lambda_tokenizer_inline_declaration(self):
        cleaned = LambdaTokenizer.get_expression(lambda: Car.id > 0)
        self.assertEqual(cleaned, "Car.id > 0")

    def test_lambda_tokenizer_inline_declaration_with_nested_parentheses(self):
        cleaned = LambdaTokenizer.get_expression(lambda: (Car.id < 0 and Car.id > 0) and (Car.id == 0 or (Car.id != 0 and Car.id >= 0)))
        self.assertEqual(cleaned, "(Car.id < 0 and Car.id > 0) and (Car.id == 0 or (Car.id != 0 and Car.id >= 0))")

    def test_lambda_tokenizer_in_object(self):
        obj = { 
            "x": lambda: Car.id > 0 
        }
        cleaned = LambdaTokenizer.get_expression(obj["x"])
        self.assertEqual(cleaned, "Car.id > 0")


if __name__ == "__main__":
    unittest.main()