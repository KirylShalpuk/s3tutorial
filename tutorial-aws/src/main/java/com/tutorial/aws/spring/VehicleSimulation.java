package com.tutorial.aws.spring;

import java.util.ArrayList;

public class VehicleSimulation {

	private Car car;
	private ArrayList<Person> passengers;
	private Person driver;
	
	public VehicleSimulation addCar(Car theCar) {
		this.car = theCar;
		return this;
	}
	
	
	
	public static void main(String[] args) {
	

	}
	
	
	class Person {
		private String name;
		private int age;
		private boolean isDriver;
	}

	class Car {
		private String brand;
		private int mileage;
	}
	
}
