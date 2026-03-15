import os
from collections import defaultdict
from decimal import Decimal

from models import Car, CarFullInfo, CarStatus, Model, ModelSaleStats, Sale
from config import RECORD_LEN
from storage import (
    append_record,
    find_line_number,
    iter_records,
    read_record_by_line,
    rebuild_index,
    rewrite_file,
    serialize,
)


class CarService:
    def __init__(self, root_directory_path: str) -> None:
        self.root_directory_path = root_directory_path

        self.cars_file = os.path.join(root_directory_path, "cars.txt")
        self.cars_index = os.path.join(root_directory_path, "cars_index.txt")

        self.models_file = os.path.join(root_directory_path, "models.txt")
        self.models_index = os.path.join(root_directory_path, "models_index.txt")

        self.sales_file = os.path.join(root_directory_path, "sales.txt")
        self.sales_index = os.path.join(root_directory_path, "sales_index.txt")

    # Задание 1. Сохранение автомобилей и моделей
    def add_model(self, model: Model) -> Model:
        append_record(self.models_file, model)
        rebuild_index(self.models_file, self.models_index, Model)
        return model

    # Задание 1. Сохранение автомобилей и моделей
    def add_car(self, car: Car) -> Car:
        model_line = find_line_number(self.models_index, str(car.model))

        if model_line is None:
            raise ValueError(f"Модель с id={car.model} не найдена")

        append_record(self.cars_file, car)
        rebuild_index(self.cars_file, self.cars_index, Car)
        return car

    # Задание 2. Сохранение продаж.
    def sell_car(self, sale: Sale) -> Car:
        car_line = find_line_number(self.cars_index, sale.car_vin)

        if car_line is None:
            raise ValueError(f"Автомобиль с VIN={sale.car_vin} не найден")

        car = read_record_by_line(self.cars_file, car_line, Car)

        if car.status in (CarStatus.sold, CarStatus.delivery):
            raise ValueError(
                f"Автомобиль с VIN={sale.car_vin} недоступен для продажи "
                f"(текущий статус: {car.status})"
            )

        append_record(self.sales_file, sale)
        rebuild_index(self.sales_file, self.sales_index, Sale)

        car.status = CarStatus.sold

        with open(self.cars_file, "r+", encoding="utf-8", newline="\n") as f:
            f.seek((car_line - 1) * RECORD_LEN)
            f.write(serialize(car))

        return car

    # Задание 3. Доступные к продаже
    def get_cars(self, status: CarStatus) -> list[Car]:
        result = []

        for _line_number, car in iter_records(self.cars_file, Car):
            if car.status == status:
                result.append(car)

        return result

    # Задание 4. Детальная информация
    def get_car_info(self, vin: str) -> CarFullInfo | None:
        car_line = find_line_number(self.cars_index, vin)

        if car_line is None:
            return None

        car = read_record_by_line(self.cars_file, car_line, Car)

        model_line = find_line_number(self.models_index, str(car.model))
        model = read_record_by_line(self.models_file, model_line, Model)

        sale_date = None
        sale_cost = None

        if car.status == CarStatus.sold:
            for _line_number, sale in iter_records(self.sales_file, Sale):
                if sale.car_vin == vin:
                    sale_date = sale.sales_date
                    sale_cost = sale.cost
                    break

        return CarFullInfo(
            vin=car.vin,
            car_model_name=model.name,
            car_model_brand=model.brand,
            price=car.price,
            date_start=car.date_start,
            status=car.status,
            sales_date=sale_date,
            sales_cost=sale_cost,
        )

    # Задание 5. Обновление ключевого поля
    def update_vin(self, vin: str, new_vin: str) -> Car:
        records = []
        updated_car = None

        for _line_number, car in iter_records(self.cars_file, Car):
            if car.vin == vin:
                car.vin = new_vin
                updated_car = car
            records.append(car)

        if updated_car is None:
            raise ValueError(f"Автомобиль с VIN={vin} не найден")

        rewrite_file(self.cars_file, records)
        rebuild_index(self.cars_file, self.cars_index, Car)

        return updated_car

    # Задание 6. Удаление продажи
    def revert_sale(self, sales_number: str) -> Car:
        sale_line = find_line_number(self.sales_index, sales_number)

        if sale_line is None:
            raise ValueError(f"Продажа с номером {sales_number} не найдена")

        sale = read_record_by_line(self.sales_file, sale_line, Sale)
        car_vin = sale.car_vin

        records_to_keep = []

        for _line_number, s in iter_records(self.sales_file, Sale):
            if s.sales_number != sales_number:
                records_to_keep.append(s)

        rewrite_file(self.sales_file, records_to_keep)
        rebuild_index(self.sales_file, self.sales_index, Sale)

        car_line = find_line_number(self.cars_index, car_vin)
        car = read_record_by_line(self.cars_file, car_line, Car)
        car.status = CarStatus.available

        with open(self.cars_file, "r+", encoding="utf-8", newline="\n") as f:
            f.seek((car_line - 1) * RECORD_LEN)
            f.write(serialize(car))

        return car

    # Задание 7. Самые продаваемые модели
    def top_models_by_sales(self) -> list[ModelSaleStats]:
        sales_count = defaultdict(int)
        price_sum = defaultdict(Decimal)

        for _line_number, sale in iter_records(self.sales_file, Sale):
            car_line = find_line_number(self.cars_index, sale.car_vin)
            car = read_record_by_line(self.cars_file, car_line, Car)

            sales_count[car.model] += 1
            price_sum[car.model] += car.price

        sorted_model_ids = sorted(
            sales_count.keys(),
            key=lambda model_id: (sales_count[model_id], price_sum[model_id]),
            reverse=True,
        )

        result = []

        for model_id in sorted_model_ids[:3]:
            model_line = find_line_number(self.models_index, str(model_id))
            model = read_record_by_line(self.models_file, model_line, Model)

            result.append(
                ModelSaleStats(
                    car_model_name=model.name,
                    brand=model.brand,
                    sales_number=sales_count[model_id],
                )
            )

        return result
