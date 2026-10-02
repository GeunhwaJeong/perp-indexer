-- Orders that never rested on the book.
--
-- The engine reports an order when it is posted to the book. An order that fills at once is
-- only reported as the fills it produced, so its history would have no order to show. The
-- state pipeline writes one for it: 'market' for an order made out of a taker fill, 'limit'
-- for one the engine posted. The ID of a 'market' order is above 2^128, outside the range the
-- engine's order IDs take, and is only a name: there is nothing on chain to cancel.
ALTER TABLE orders ADD COLUMN kind TEXT NOT NULL DEFAULT 'limit';
