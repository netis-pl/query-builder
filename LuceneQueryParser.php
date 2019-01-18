<?php

namespace common\components\queryBuilder;

use Gdbots\QueryParser\Enum\BoolOperator;
use Gdbots\QueryParser\QueryParser;
use Gdbots\QueryParser\Token;

class LuceneQueryParser extends QueryParser
{
    /**
     * Override default operator from OPTIONAL to REQUIRED
     *
     * @param int $default
     *
     * @return BoolOperator
     *
     * @SuppressWarnings(PHPMD.StaticAccess)
     */
    protected function getBoolOperator(int $default = BoolOperator::REQUIRED): BoolOperator
    {
        if ($this->stream->nextIf(Token::T_REQUIRED)
            || $this->stream->lookaheadTypeIs(Token::T_AND)
            || $this->stream->prevTypeIs(Token::T_AND)
        ) {
            return BoolOperator::REQUIRED();
        }

        if ($this->stream->nextIf(Token::T_OR)) {
            return BoolOperator::OPTIONAL();
        }

        if ($this->stream->nextIf(Token::T_PROHIBITED)) {
            return BoolOperator::PROHIBITED();
        }

        return BoolOperator::create($default);
    }
}
