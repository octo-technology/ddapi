def generate_table_name(context, task_name):
    workflow = context.workflow
    return workflow.name + "_" + task_name + "_" + workflow.task_id_counter
