import unittest

from mock import MagicMock

from dd.api.contexts.local import LocalContext
from dd.api.workflow.actions import Action
from dd.api.workflow.callables import ActionCallableBuilder


class TestActions(unittest.TestCase):
    def setUp(self):
        self.dataset = MagicMock()
        self.context = MagicMock()
        self.context.dependency_model = False
        self.mock_function = MagicMock()
        self.mock_function.__name__ = "mock"
        self.callable_builder = (ActionCallableBuilder(self.context)
                                 .with_operation(self.mock_function))

    def new_action(self):
        return Action(self.context, self.callable_builder, self.dataset)

    def test_actions_should_create_new_operator(self):
        # Given
        context = self.context
        callable_builder = self.callable_builder
        dataset = self.dataset

        # When
        _ = Action(context, callable_builder, dataset)

        # Check
        context.build_operator.assert_called_once()

    def test_actions_should_be_able_to_repr_themselves(self):
        # Given
        context = MagicMock()
        context.name = "Mock"
        context.uuid = "uuid"
        context.task_id_counter = 0

        dataset = MagicMock()
        dataset.output_table = "Dataset"

        builder = MagicMock()
        builder.bind_to().build().__name__ = "foo"
        action = Action(context, builder, dataset)

        # When
        r = repr(action)

        # Check
        self.assertEqual(r, "MockAction[foo_0_uuid][0]")

    def test_execute_should_call_predecessors_when_recursive(self):
        # Given
        action = self.new_action()

        # When
        action.execute(force=True, recursive=True)

        # Check
        self.dataset.execute.assert_called_once_with(True, True)

    def test_execute_should_not_call_predecessors_when_not_recursive(self):
        # Given
        action = self.new_action()

        # When
        action.execute(force=True, recursive=False)

        # Check
        self.dataset.execute.assert_not_called()

    def test_create_new_action_should_increment_context_task_id_counter(self):
        # Given
        db = MagicMock()
        context = LocalContext(db)
        initial_task_id_counter_value = context.task_id_counter
        # When
        Action(context, self.callable_builder, self.dataset)

        # Then
        self.assertEqual(context.task_id_counter, initial_task_id_counter_value+1)
