"""Run the real main AST offline to verify holiday/error startup behavior."""
import ast
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


class StartupCalendarTests(unittest.TestCase):
    def test_calendar_precedes_state_sync_and_login(self):
        path = Path(__file__).parent / "straddle" / "straddle.py"
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        self.assertTrue(any(isinstance(n, ast.ImportFrom)
                            and n.module == "tamingnifty.safety"
                            and any(a.name == "check_trading_day" for a in n.names)
                            for n in tree.body))
        main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
        startup = next(n for n in main.body if isinstance(n, ast.Try))
        gate = startup.body[0]
        self.assertIsInstance(gate, ast.If)
        self.assertIn("check_trading_day", ast.unparse(gate.test))
        for node in ast.walk(main):
            if isinstance(node, ast.Call) and node.lineno < gate.lineno:
                self.assertIn(ast.unparse(node.func), ("clients", "notify"))
        for outcome in (True, False, ValueError("missing/invalid calendar"),
                        RuntimeError("calendar database unavailable")):
            with self.subTest(outcome=outcome):
                mongo, slack, notify, after = Mock(), Mock(), Mock(), Mock()
                def check(client, callback):
                    self.assertIs(client, mongo)
                    callback("calendar status")
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome
                startup.body = [gate] + ast.parse("after_gate()").body
                check_mock = Mock(side_effect=check)
                namespace = dict(clients=lambda: (mongo, Mock(), Mock(), slack),
                                 STRATEGY_VERSION="test", notify=notify,
                                 check_trading_day=check_mock, after_gate=after,
                                 util=SimpleNamespace(exception_detail=str),
                                 traceback=SimpleNamespace(print_exc=Mock()))
                exec(compile(ast.fix_missing_locations(
                    ast.Module(body=[main], type_ignores=[])), str(path), "exec"), namespace)
                if isinstance(outcome, Exception):
                    with self.assertRaises(type(outcome)):
                        namespace["main"]()
                else:
                    self.assertIsNone(namespace["main"]())
                check_mock.assert_called_once()
                self.assertIn(((slack, "calendar status"), {}), notify.call_args_list)
                self.assertEqual(after.call_count, int(outcome is True))
                mongo.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
