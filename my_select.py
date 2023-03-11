from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Group, Discipline, Grade
from src.db import session


def select_1():
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2(discipline_id: int):
    result = session.query(Discipline.name, Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Grade).join(Student).join(Discipline).filter(Discipline.id == discipline_id)\
                    .group_by(Student.id, Discipline.id).order_by(desc('avg_grade')).limit(1).all()
    return result


def select_3(discipline_id: int):
    result = session.query(Discipline.name, Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Grade).join(Student).join(Discipline).join(Group)\
                    .filter(Discipline.id == discipline_id).group_by(Group.id, Discipline.id)\
                    .order_by(desc('avg_grade')).all()
    return result


def select_4():
    result = session.query(func.round(func.avg(Grade.grade), 2)).select_from(Grade).all()
    return result


def select_5(teacher_id: int):
    result = session.query(Discipline.name, Teacher.fullname).select_from(Discipline).join(Teacher)\
                    .filter(Teacher.id == teacher_id).order_by(Discipline.name).all()
    return result


def select_6(group_id: int):
    result = session.query(Student.fullname, Group.name).select_from(Student).join(Group).filter(Group.id == group_id)\
                    .order_by(Student.fullname).all()
    return result


def select_7(group_id: int, discipline_id: int):
    result = session.query(Student.fullname, Group.name, Discipline.name, Grade.grade).select_from(Grade).join(Student)\
                    .join(Group).join(Discipline).filter(Group.id == group_id, Discipline.id == discipline_id)\
                    .order_by(Student.fullname).all()
    return result


def select_8(teacher_id: int):
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade'), Discipline.name, Teacher.fullname)\
                    .select_from(Grade).join(Discipline).join(Teacher)\
                    .filter(Discipline.teacher_id == teacher_id, Teacher.id == teacher_id)\
                    .group_by(Discipline.name, Teacher.fullname).all()
    return result


def select_9(student_id: int):
    result = session.query(Discipline.name, Student.fullname).select_from(Grade).join(Discipline).join(Student)\
                    .filter(Student.id == student_id).group_by(Discipline.name, Student.fullname).all()
    return result


def select_10(student_id: int, teacher_id: int):
    result = session.query(Discipline.name).select_from(Grade).join(Discipline).join(Teacher).join(Student)\
                    .filter(Student.id == student_id, Teacher.id == teacher_id).group_by(Discipline.name).all()
    return result


def select_11(teacher_id: int, student_id: int):
    result = session.query(func.round(func.avg(Grade.grade), 2), Student.fullname, Teacher.fullname).select_from(Grade)\
                    .join(Student).join(Discipline).join(Teacher)\
                    .filter(Student.id == student_id, Teacher.id == teacher_id)\
                    .group_by(Student.fullname, Teacher.fullname).all()
    return result


def select_12(discipline_id, group_id):
    subquery = (select(Grade.date_of).join(Student).join(Group)\
                .where(and_(Grade.discipline_id == discipline_id, Group.id == group_id)).order_by(desc(Grade.date_of))\
                .limit(1).scalar_subquery())

    result = session.query(Discipline.name, Student.fullname, Group.name, Grade.date_of, Grade.grade)\
                    .select_from(Grade).join(Student).join(Discipline).join(Group)\
                    .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery))\
                    .order_by(desc(Grade.date_of)).all()
    return result


if __name__ == '__main__':
    print(select_1())
    print(select_2(2))
    print(select_3(2))
    print(select_4())
    print(select_5(3))
    print(select_6(1))
    print(select_7(1, 5))
    print(select_8(2))
    print(select_9(25))
    print(select_10(14, 2))
    print(select_11(1, 30))
    print(select_12(2, 3))
