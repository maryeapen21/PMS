# PMS Back end code
import psycopg2
from psycopg2 import sql
from datetime import date, timedelta
from typing import List, Dict, Any

# Database connection credentials - REPLACE WITH YOUR OWN
DB_HOST = "localhost"
DB_NAME = "PMS"
DB_USER = "postgres"
DB_PASS = "1234"

def create_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error: Unable to connect to the database. {e}")
        return None

def create_tables():
    """Creates necessary tables for the PMS application if they do not exist."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    employee_id SERIAL PRIMARY KEY,
                    employee_name VARCHAR(100) NOT NULL,
                    role VARCHAR(50) NOT NULL CHECK (role IN ('Manager', 'Employee')),
                    manager_id INTEGER REFERENCES employees(employee_id)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS goals (
                    goal_id SERIAL PRIMARY KEY,
                    employee_id INTEGER REFERENCES employees(employee_id),
                    description TEXT NOT NULL,
                    due_date DATE NOT NULL,
                    status VARCHAR(50) NOT NULL CHECK (status IN ('Draft', 'In Progress', 'Completed', 'Cancelled')),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id SERIAL PRIMARY KEY,
                    goal_id INTEGER REFERENCES goals(goal_id) ON DELETE CASCADE,
                    employee_id INTEGER REFERENCES employees(employee_id),
                    description TEXT NOT NULL,
                    is_approved BOOLEAN DEFAULT FALSE,
                    completed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP WITH TIME ZONE
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS feedback (
                    feedback_id SERIAL PRIMARY KEY,
                    goal_id INTEGER REFERENCES goals(goal_id) ON DELETE CASCADE,
                    manager_id INTEGER REFERENCES employees(employee_id),
                    employee_id INTEGER REFERENCES employees(employee_id),
                    feedback_text TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error creating tables: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    return False

# CRUD Operations for Employees
def create_employee(employee_name: str, role: str, manager_id: int = None) -> bool:
    """Creates a new employee in the database."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO employees (employee_name, role, manager_id) VALUES (%s, %s, %s)",
                (employee_name, role, manager_id)
            )
            conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error creating employee: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    return False

def read_employee_by_id(employee_id: int):
    """Reads a single employee's details by their ID."""
    conn = create_connection()
    employee = None
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT employee_id, employee_name, role FROM employees WHERE employee_id = %s", (employee_id,))
            employee = cursor.fetchone()
        except psycopg2.Error as e:
            print(f"Error reading employee: {e}")
        finally:
            cursor.close()
            conn.close()
    return employee

# CRUD Operations for Goals
def create_goal(employee_id: int, description: str, due_date: date, status: str = 'Draft') -> bool:
    """Creates a new goal for a specific employee."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO goals (employee_id, description, due_date, status) VALUES (%s, %s, %s, %s)",
                (employee_id, description, due_date, status)
            )
            conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error creating goal: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    return False

def read_goals(employee_id: int) -> List[Dict[str, Any]]:
    """Reads all goals for a specific employee."""
    conn = create_connection()
    goals = []
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT goal_id, description, due_date, status, created_at FROM goals WHERE employee_id = %s ORDER BY created_at DESC",
                (employee_id,)
            )
            goals = [
                {'goal_id': row[0], 'description': row[1], 'due_date': row[2], 'status': row[3], 'created_at': row[4]}
                for row in cursor.fetchall()
            ]
        except psycopg2.Error as e:
            print(f"Error reading goals: {e}")
        finally:
            cursor.close()
            conn.close()
    return goals

def update_goal(goal_id: int, description: str, due_date: date, status: str) -> bool:
    """Updates an existing goal and adds a feedback entry if the status is 'Completed'."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE goals SET description = %s, due_date = %s, status = %s WHERE goal_id = %s",
                (description, due_date, status, goal_id)
            )
            
            # Automated feedback trigger
            if status == 'Completed':
                cursor.execute("SELECT employee_id FROM goals WHERE goal_id = %s", (goal_id,))
                employee_id = cursor.fetchone()[0]
                
                # For this simple example, we'll assume a dummy manager_id
                manager_id = 1 
                automated_feedback = f"Automated feedback: Goal '{description}' was completed on time."
                
                cursor.execute(
                    "INSERT INTO feedback (goal_id, manager_id, employee_id, feedback_text) VALUES (%s, %s, %s, %s)",
                    (goal_id, manager_id, employee_id, automated_feedback)
                )

            conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error updating goal: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    return False

def delete_goal(goal_id: int) -> bool:
    """Deletes a goal and all associated tasks and feedback."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM goals WHERE goal_id = %s", (goal_id,))
            conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error deleting goal: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    return False

# CRUD Operations for Tasks
def create_task(goal_id: int, employee_id: int, description: str) -> bool:
    """Adds a new task to a goal."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO tasks (goal_id, employee_id, description) VALUES (%s, %s, %s)",
                (goal_id, employee_id, description)
            )
            conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error creating task: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    return False

def read_tasks(goal_id: int) -> List[Dict[str, Any]]:
    """Reads all tasks for a specific goal."""
    conn = create_connection()
    tasks = []
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT task_id, description, is_approved, completed, created_at, completed_at FROM tasks WHERE goal_id = %s ORDER BY created_at ASC",
                (goal_id,)
            )
            tasks = [
                {'task_id': row[0], 'description': row[1], 'is_approved': row[2], 'completed': row[3], 'created_at': row[4], 'completed_at': row[5]}
                for row in cursor.fetchall()
            ]
        except psycopg2.Error as e:
            print(f"Error reading tasks: {e}")
        finally:
            cursor.close()
            conn.close()
    return tasks

def update_task_status(task_id: int, completed: bool, is_approved: bool) -> bool:
    """Updates the status of a task."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE tasks SET completed = %s, is_approved = %s, completed_at = %s WHERE task_id = %s",
                (completed, is_approved, date.today() if completed else None, task_id)
            )
            conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error updating task status: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    return False
    
# CRUD Operations for Feedback
def create_feedback(goal_id: int, manager_id: int, employee_id: int, feedback_text: str) -> bool:
    """Adds feedback to a specific goal for an employee."""
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO feedback (goal_id, manager_id, employee_id, feedback_text) VALUES (%s, %s, %s, %s)",
                (goal_id, manager_id, employee_id, feedback_text)
            )
            conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error creating feedback: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    return False

def read_feedback(goal_id: int) -> List[Dict[str, Any]]:
    """Reads all feedback for a specific goal."""
    conn = create_connection()
    feedbacks = []
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT feedback_id, feedback_text, created_at FROM feedback WHERE goal_id = %s ORDER BY created_at DESC",
                (goal_id,)
            )
            feedbacks = [
                {'feedback_id': row[0], 'feedback_text': row[1], 'created_at': row[2]}
                for row in cursor.fetchall()
            ]
        except psycopg2.Error as e:
            print(f"Error reading feedback: {e}")
        finally:
            cursor.close()
            conn.close()
    return feedbacks

# Business Insights & Reporting
def get_pms_metrics(employee_id: int) -> Dict[str, Any]:
    """Calculates and returns key business metrics for the dashboard."""
    conn = create_connection()
    metrics = {}
    if conn:
        cursor = conn.cursor()
        try:
            # COUNT of goals by status
            cursor.execute("SELECT status, COUNT(*) FROM goals WHERE employee_id = %s GROUP BY status", (employee_id,))
            metrics['goal_counts'] = dict(cursor.fetchall())
            
            # SUM of completed tasks
            cursor.execute("SELECT COUNT(*) FROM tasks WHERE employee_id = %s AND completed = TRUE", (employee_id,))
            metrics['completed_tasks'] = cursor.fetchone()[0] or 0

            # Total number of goals
            cursor.execute("SELECT COUNT(*) FROM goals WHERE employee_id = %s", (employee_id,))
            metrics['total_goals'] = cursor.fetchone()[0] or 0
            
            # Average time to complete a task
            cursor.execute("""
                SELECT AVG(completed_at - created_at) FROM tasks 
                WHERE employee_id = %s AND completed = TRUE AND completed_at IS NOT NULL
            """, (employee_id,))
            avg_completion_delta = cursor.fetchone()[0]
            metrics['avg_task_completion_days'] = avg_completion_delta.days if avg_completion_delta else 0

            # MIN and MAX goal due dates
            cursor.execute("SELECT MIN(due_date), MAX(due_date) FROM goals WHERE employee_id = %s", (employee_id,))
            min_due_date, max_due_date = cursor.fetchone()
            metrics['min_due_date'] = min_due_date.isoformat() if min_due_date else "N/A"
            metrics['max_due_date'] = max_due_date.isoformat() if max_due_date else "N/A"

        except psycopg2.Error as e:
            print(f"Error fetching PMS metrics: {e}")
        finally:
            cursor.close()
            conn.close()
    return metrics



