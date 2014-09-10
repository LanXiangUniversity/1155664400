package structural;

/**
 * Created by Wei on 9/1/14.
 */
public class DecoratorPattern {
	public static void main(String[] args) {
		ConcreteDecorator concreteDecorator = new ConcreteDecorator(new ConcreteComponent());
		concreteDecorator.operation();

		People people = new Chinese();
		people = new Art(people);
		people.speak();

		people = new American();
		people = new Sports(people);
		people.speak();
	}
}

interface VisualComponent {
	public void operation();
}

class ConcreteComponent implements VisualComponent {

	@Override
	public void operation() {
		System.out.println("Concrete Component");
	}
}

class Decorator implements VisualComponent {
	private VisualComponent component;

	public Decorator(VisualComponent component) {
		this.component = component;
	}

	public void operation() {
		component.operation();
	}
}

class ConcreteDecorator extends Decorator {
	public ConcreteDecorator(VisualComponent component) {
		super(component);
	}

	@Override
	public void operation() {
		super.operation();

		this.addBehavior();
	}

	private void addBehavior() {
		System.out.println("addBehavior");
	}
}


/**
 * *******************************************
 */
interface People {
	public void speak();
}

// Racial

class Chinese implements People {

	@Override
	public void speak() {
		System.out.print("你好\t");
	}
}

class American implements People {

	@Override
	public void speak() {
		System.out.print("Hello\t");
	}
}

// Hobby

class Hobby implements People {
	private People people;

	public Hobby(People people) {
		this.people = people;
	}

	public void speak() {
		people.speak();
	}
}

class Art extends Hobby {
	public Art(People people) {
		super(people);
	}

	@Override
	public void speak() {
		super.speak();

		this.draw();
	}

	private void draw() {
		System.out.println("draw");
	}
}

class Sports extends Hobby {
	public Sports(People people) {
		super(people);
	}

	@Override
	public void speak() {
		super.speak();

		this.play();
	}

	private void play() {
		System.out.println("play football");
	}
}



