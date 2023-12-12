from conductor.client.http.models import Task, TaskResult
from conductor.client.http.models.task_result_status import TaskResultStatus
from conductor.client.worker.worker_interface import WorkerInterface
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.settings.authentication_settings import AuthenticationSettings
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.worker.worker import Worker
from conductor.client.worker.worker_task import WorkerTask

### Add these lines if not running on unix###
from multiprocessing import set_start_method
set_start_method("fork")
#############################################

SERVER_API_URL = 'https://play.orkes.io/api'
KEY_ID = 'dccbffd7-bab3-4f50-8aaf-d05cb489f3ce'
KEY_SECRET = 'SFuC2GjB0RqYrvczVRX6osWj8hpmaNRDK3Z8oTMVW9XRuoke'
TASK_DEFINITION_NAME = 'JSS_sumNumbers'

# Create worker using a class implemented by WorkerInterface
class SimplePythonWorker(WorkerInterface):
    def execute(self, task: Task) -> TaskResult:
        # -----------------------------------------------------------------------------------
        # 
        # Performs the actual comparison
        # 
        # -----------------------------------------------------------------------------------
        numbers = task.input_data["numbers"]
        target = task.input_data["target"]
        result = []
        for x in range(len(numbers)):
            for y in (range(x+1, len(numbers))):
                if ((numbers[x] + numbers[y]) == target):
                    result = [x,y]
        #
        # Save the results into the task_results and return
        #
        task_result = self.get_task_result_from_task(task)
        task_result.add_output_data('result', result)
        task_result.add_output_data('language', 'Python3')
        task_result.status = TaskResultStatus.COMPLETED
        return task_result

    def get_polling_interval_in_seconds(self) -> float:
        # poll every 500ms
        return 10

# Create worker using a function
def execute(task: Task) -> TaskResult:
    task_result = TaskResult(
        task_id=task.task_id,
        workflow_instance_id=task.workflow_instance_id,
        worker_id='your_custom_id'
    )
    task_result.add_output_data('worker_style', 'function')
    task_result.status = TaskResultStatus.COMPLETED
    return task_result

configuration = Configuration(
    server_api_url=SERVER_API_URL,
    debug=True,
    authentication_settings=AuthenticationSettings(
        key_id=KEY_ID,
        key_secret=KEY_SECRET
    ),
)

workers = [
    SimplePythonWorker(
        task_definition_name=TASK_DEFINITION_NAME
    ),
]

# If there are decorated workers in your application, scan_for_annotated_workers should be set
# default value of scan_for_annotated_workers is False
with TaskHandler(workers, configuration, scan_for_annotated_workers=True) as task_handler:
    task_handler.start_processes()
    task_handler.join_processes()
