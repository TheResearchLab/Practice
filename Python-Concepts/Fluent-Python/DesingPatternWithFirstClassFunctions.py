#%%

from abc import ABC, abstractmethod
from collections.abc import Sequence
from decimal import Decimal
from typing import NamedTuple, Optional 

class Customer(NamedTuple):
    name: str
    fidelity: int 

class LineItem(NamedTuple):
    product: str
    quantity: int
    price: Decimal 

    def total(self) -> Decimal:
        return self.price * self.quantity
    
class Order(NamedTuple): # context
    customer: Customer
    cart: Sequence[LineItem]
    promotion: Optional['Promotion'] = None

    def total(self) -> Decimal:
        totals = (item.total() for item in self.cart)
        return sum(totals,start=Decimal(0))
    
    def due(self) -> Decimal:
        if self.promotion is None:
            discount = Decimal(0)
        else:
            discount = self.promotion.discount(self)
        return self.total() - discount
    
    def __repr__(self):
        return f'<Order total: {self.total():.2f} due: {self.due():.2f}>'

class Promotion(ABC):
    @abstractmethod
    def discount(self, order:Order) -> Decimal:
        """ Returns discount as a positive dollar amount """

class FidelityPromo(Promotion):
    """ 5% discount for customers with 1000 or more fidelity points"""

    def discount(self,order:Order) -> Decimal:
        rate = Decimal('0.05')
        if order.customer.fidelity >= 1000:
            return order.total() * rate 
        return Decimal(0)
    
class BulkItemPromo(Promotion):
    """10% discount for LineItem with 20 or more units"""

    def discount(self, order: Order) -> Decimal:
        discount = Decimal(0)
        for item in order.cart:
            if item.quantity >= 20:
                discount += item.total() * Decimal('0.1')
        return discount 
    
class LargeOrderPromo(Promotion):
    """7% discount for order with 10 or more distinct items"""

    def discount(self,order:Order) -> Decimal:
        distinct_items = {item.product for item in order.cart}
        if len(distinct_items) >= 10:
            return order.total() * Decimal('0.07')
        return Decimal(0)
    


# FUNCTION ORIENTED STRATEGY

from dataclasses import dataclass
from typing import Callable

class Customer(NamedTuple):
    name: str
    fidelity: int 

class LineItem(NamedTuple):
    product: str
    quantity: int
    price: Decimal 

    def total(self) -> Decimal:
        return self.price * self.quantity

@dataclass(frozen=True)   
class Order(NamedTuple): # context
    customer: Customer
    cart: Sequence[LineItem]
    promotion: Optional[Callable[['Order'],Decimal]] = None

    def total(self) -> Decimal:
        totals = (item.total() for item in self.cart)
        return sum(totals,start=Decimal(0))
    
    def due(self) -> Decimal:
        if self.promotion is None:
            discount = Decimal(0)
        else:
            discount = self.promotion(self)
        return self.total() - discount
    
    def __repr__(self):
        return f'<Order total: {self.total():.2f} due: {self.due():.2f}>'


def fidelity_promo(order:Order) -> Decimal:
    """ 5% discount for customers with 1000 or more fidelity points"""
    rate = Decimal('0.05')
    if order.customer.fidelity >= 1000:
        return order.total() * rate 
    return Decimal(0)
    


def bulk_item_promo(order: Order) -> Decimal:
    """10% discount for LineItem with 20 or more units"""
    discount = Decimal(0)
    for item in order.cart:
        if item.quantity >= 20:
            discount += item.total() * Decimal('0.1')
    return discount 
    


def larger_order_promo(order:Order) -> Decimal:
    """7% discount for order with 10 or more distinct items"""
    distinct_items = {item.product for item in order.cart}
    if len(distinct_items) >= 10:
        return order.total() * Decimal('0.07')
    return Decimal(0)
     

# Addition optimizations

promos = [fidelity_promo, bulk_item_promo, larger_order_promo]

def best_promo(order: Order) -> Decimal:
    """Compute the best discount available"""
    return max(promo(order) for promo in promos)


# PROMOTION DECORATOR

Promotion = Callable[[Order],Decimal]

promos: list[Promotion] = []

def promotion(promo:Promotion) -> Promotion:
    promos.append(promo)
    return promo 

def best_promo(order:Order) -> Decimal:
    """Compute the best discount available"""
    return max(promo(order) for promo in promos)

@promotion
def fidelity(order:Order) -> Decimal:
    """5% discount for customer with 1000 or more fidelity points"""
    if order.customer.fidelity >= 1000:
        return order.total() * Decimal('0.05')
    return Decimal(0)

@promotion
def bulk_item(order:Order) -> Decimal:
    """10% discount for each LineItem with 20 or more units"""
    discount = Decimal(0)
    for item in order.cart:
        if item.quantity >= 20:
            discount += item.total() * Decimal('0.1')
    return discount 

@promotion
def large_order(order:Order) -> Decimal:
    """7% discount for orders with 10 or more distinct items"""
    distinct_items = {item.product for item in order.cart} # set comprehension
    if len(distinct_items) >= 10:
        return order.total() * Decimal('0.07')
    return Decimal(0)


# COMMAND PATTERN 
class MacroCommand:
    def __init__(self,commands):
        self.commands = list(commands)
    
    def __call__(self):
        for command in self.commands:
            command()

