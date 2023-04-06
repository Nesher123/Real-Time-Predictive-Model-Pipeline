import os
import pickle


def get_project_root() -> str:
    """
    Get the root directory of the project
    :return: the root directory of the project
    """

    current_dir = os.path.abspath(os.curdir)  # Get the current working directory
    root_dir = current_dir

    # Traverse the directory tree upwards until you find a desired marker, e.g., .git, or any other unique identifier
    while not os.path.exists(os.path.join(root_dir, ".git")):
        root_dir = os.path.dirname(root_dir)  # Move up one level in the directory tree

        if os.path.abspath(root_dir) == os.path.abspath(os.path.dirname(root_dir)):
            # We've reached the root directory of the file system, break the loop
            break

    return root_dir


def save_model(model, model_name: str) -> None:
    """
    Save the model to disk

    :param model: the model to save
    :param model_name: the name of the model
    :return: None
    """

    project_root = get_project_root()
    model_dir = os.path.join(project_root, "src", "model")

    # create the model_dir folder if it doesn't exist
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    model_filename = os.path.join(model_dir, model_name)

    # save the model to disk
    with open(model_filename, "wb") as f:
        pickle.dump(model, f)
